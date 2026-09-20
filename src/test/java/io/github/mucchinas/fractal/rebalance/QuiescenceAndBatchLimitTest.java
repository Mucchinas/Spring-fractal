package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.config.FractalProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class QuiescenceAndBatchLimitTest {

    private DataSource primaryDs;
    private DataSource shard1Ds;
    private DataSource shard2Ds;
    private TopologyManager topologyManager;

    @BeforeEach
    void setup() {
        primaryDs = createDataSource("quiescence_prim");
        shard1Ds = createDataSource("quiescence_s1");
        shard2Ds = createDataSource("quiescence_s2");

        topologyManager = new TopologyManager(primaryDs, true);
        topologyManager.initializeSchema();
    }

    @AfterEach
    void tearDown() {
        topologyManager.destroy();
    }

    private DataSource createDataSource(String name) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        ds.setUsername("sa");
        ds.setPassword("");
        return ds;
    }

    @Test
    void testInFlightRequestTrackingAndQuiescence() throws InterruptedException {
        String tenantId = "tenant-fast";
        topologyManager.registerRequestStart(tenantId);
        assertThat(topologyManager.getInFlightRequestCount(tenantId)).isEqualTo(1);

        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            } finally {
                topologyManager.registerRequestEnd(tenantId);
                latch.countDown();
            }
        }).start();

        boolean drained = topologyManager.awaitTenantQuiescence(tenantId, Duration.ofMillis(800));
        assertThat(drained).isTrue();
        assertThat(topologyManager.getInFlightRequestCount(tenantId)).isEqualTo(0);
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testQuiescenceTimeoutOnActiveRequest() {
        String tenantId = "tenant-blocked";
        topologyManager.registerRequestStart(tenantId);
        try {
            boolean drained = topologyManager.awaitTenantQuiescence(tenantId, Duration.ofMillis(100));
            assertThat(drained).isFalse();
        } finally {
            topologyManager.registerRequestEnd(tenantId);
        }
        assertThat(topologyManager.getInFlightRequestCount(tenantId)).isEqualTo(0);
    }

    @Test
    void testDynamicBatchSizeCalculation() {
        FractalProperties.RebalancerProperties props = new FractalProperties.RebalancerProperties();
        props.setBatchSize(500);
        props.setMaxBatchParameters(32766);
        assertThat(props.calculateBatchSize(5)).isEqualTo(500);
        assertThat(props.calculateBatchSize(10)).isEqualTo(500);
        assertThat(props.calculateBatchSize(140)).isEqualTo(234);
        assertThat(props.calculateBatchSize(500)).isEqualTo(65);
        props.setMaxBatchParameters(2000);
        assertThat(props.calculateBatchSize(50)).isEqualTo(40);
        assertThat(props.calculateBatchSize(0)).isEqualTo(500);
        assertThat(props.calculateBatchSize(-1)).isEqualTo(500);
    }

    @Test
    void testRebalanceDefersWhenInFlightRequestsRefuseToDrain() {
        JdbcTemplate prim = new JdbcTemplate(primaryDs);
        prim.execute("CREATE TABLE users (id VARCHAR(50) PRIMARY KEY, status VARCHAR(20))");
        prim.execute("INSERT INTO users VALUES ('user-busy', 'ACTIVE')");

        JdbcTemplate s1 = new JdbcTemplate(shard1Ds);
        s1.execute("CREATE TABLE users (id VARCHAR(50) PRIMARY KEY, status VARCHAR(20))");
        s1.execute("INSERT INTO users VALUES ('user-busy', 'ACTIVE')");

        JdbcTemplate s2 = new JdbcTemplate(shard2Ds);
        s2.execute("CREATE TABLE users (id VARCHAR(50) PRIMARY KEY, status VARCHAR(20))");

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setEnabled(true);
        properties.getRebalancer().setRootTable("users");
        properties.getRebalancer().setRootIdColumn("id");
        properties.getRebalancer().setStatusColumn("status");
        properties.getRebalancer().setShardedTables(List.of("users"));
        properties.getRebalancer().setDrainTimeout(Duration.ofMillis(100));

        TableDependencyResolver resolver = new TableDependencyResolver(primaryDs);
        Map<String, DataSource> shardMap = Map.of("shard-1", shard1Ds, "shard-2", shard2Ds);
        RebalanceEngine engine = new RebalanceEngine(primaryDs, shardMap, resolver, topologyManager, properties.getRebalancer());
        topologyManager.registerRequestStart("user-busy");
        try {
            MigrationDeltaCalculator.MigrationAction action =
                    new MigrationDeltaCalculator.MigrationAction("user-busy", "shard-1", "shard-2");
            engine.executeMigration(List.of(action));
            Integer s1Count = s1.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-busy'", Integer.class);
            Integer s2Count = s2.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-busy'", Integer.class);
            assertThat(s1Count).isEqualTo(1);
            assertThat(s2Count).isEqualTo(0);
            String status = prim.queryForObject("SELECT status FROM users WHERE id = 'user-busy'", String.class);
            assertThat(status).isEqualTo("ACTIVE");
        } finally {
            topologyManager.registerRequestEnd("user-busy");
        }
    }

    @Test
    void testWideTableMigrationWithCalculatedBatchSize() {
        JdbcTemplate prim = new JdbcTemplate(primaryDs);
        prim.execute("CREATE TABLE accounts (id VARCHAR(50) PRIMARY KEY, status VARCHAR(20), c1 VARCHAR(10), c2 VARCHAR(10), c3 VARCHAR(10), c4 VARCHAR(10))");
        prim.execute("INSERT INTO accounts VALUES ('acc-1', 'ACTIVE', 'v1', 'v2', 'v3', 'v4')");

        JdbcTemplate s1 = new JdbcTemplate(shard1Ds);
        s1.execute("CREATE TABLE accounts (id VARCHAR(50) PRIMARY KEY, status VARCHAR(20), c1 VARCHAR(10), c2 VARCHAR(10), c3 VARCHAR(10), c4 VARCHAR(10))");
        s1.execute("INSERT INTO accounts VALUES ('acc-1', 'ACTIVE', 'v1', 'v2', 'v3', 'v4')");

        JdbcTemplate s2 = new JdbcTemplate(shard2Ds);
        s2.execute("CREATE TABLE accounts (id VARCHAR(50) PRIMARY KEY, status VARCHAR(20), c1 VARCHAR(10), c2 VARCHAR(10), c3 VARCHAR(10), c4 VARCHAR(10))");

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setEnabled(true);
        properties.getRebalancer().setRootTable("accounts");
        properties.getRebalancer().setRootIdColumn("id");
        properties.getRebalancer().setStatusColumn("status");
        properties.getRebalancer().setShardedTables(List.of("accounts"));
        properties.getRebalancer().setMaxBatchParameters(12);
        properties.getRebalancer().setDrainTimeout(Duration.ofMillis(200));

        TableDependencyResolver resolver = new TableDependencyResolver(primaryDs);
        Map<String, DataSource> shardMap = Map.of("shard-1", shard1Ds, "shard-2", shard2Ds);
        RebalanceEngine engine = new RebalanceEngine(primaryDs, shardMap, resolver, topologyManager, properties.getRebalancer());

        MigrationDeltaCalculator.MigrationAction action =
                new MigrationDeltaCalculator.MigrationAction("acc-1", "shard-1", "shard-2");
        engine.executeMigration(List.of(action));

        Integer s1Count = s1.queryForObject("SELECT COUNT(*) FROM accounts WHERE id = 'acc-1'", Integer.class);
        Integer s2Count = s2.queryForObject("SELECT COUNT(*) FROM accounts WHERE id = 'acc-1'", Integer.class);
        assertThat(s1Count).isEqualTo(0);
        assertThat(s2Count).isEqualTo(1);
    }
}
