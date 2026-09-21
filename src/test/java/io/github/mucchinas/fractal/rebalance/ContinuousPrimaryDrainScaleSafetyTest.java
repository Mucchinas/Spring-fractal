package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class ContinuousPrimaryDrainScaleSafetyTest {

    private DriverManagerDataSource primaryDs;
    private DriverManagerDataSource shard1Ds;
    private DriverManagerDataSource shard2Ds;
    private ConsistentHashRouter router;
    private FractalProperties.RebalancerProperties props;

    @BeforeEach
    void setUp() {
        primaryDs = createDataSource("scale_primary");
        shard1Ds = createDataSource("scale_shard1");
        shard2Ds = createDataSource("scale_shard2");

        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        primaryJdbc.execute("CREATE TABLE users (id VARCHAR(255) PRIMARY KEY, status VARCHAR(50))");

        new JdbcTemplate(shard1Ds).execute("CREATE TABLE users (id VARCHAR(255) PRIMARY KEY, status VARCHAR(50))");
        new JdbcTemplate(shard2Ds).execute("CREATE TABLE users (id VARCHAR(255) PRIMARY KEY, status VARCHAR(50))");

        router = new ConsistentHashRouter(Set.of("shard-1", "shard-2"), 150);

        props = new FractalProperties.RebalancerProperties();
        props.setRootTable("users");
        props.setRootIdColumn("id");
        props.setStatusColumn("status");
        props.setActiveValue("ACTIVE");
    }

    @AfterEach
    void tearDown() {
        dropTable(primaryDs, "users");
        dropTable(shard1Ds, "users");
        dropTable(shard2Ds, "users");
    }

    private DriverManagerDataSource createDataSource(String name) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl("jdbc:h2:mem:" + name + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        ds.setUsername("sa");
        ds.setPassword("");
        return ds;
    }

    private void dropTable(DataSource ds, String table) {
        try {
            new JdbcTemplate(ds).execute("DROP TABLE IF EXISTS " + table);
        } catch (Exception ignored) {}
    }

    @Test
    void tier1CountGuardShouldReturnEmptyWhenCountsMatch() {
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        JdbcTemplate shard1Jdbc = new JdbcTemplate(shard1Ds);
        JdbcTemplate shard2Jdbc = new JdbcTemplate(shard2Ds);

        // Seed 1,000 tenants partitioned across shard-1 and shard-2
        for (int i = 0; i < 1000; i++) {
            String tenantId = "tenant-" + String.format("%05d", i);
            primaryJdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", tenantId, "ACTIVE");
            String targetShard = router.routeNode(tenantId);
            if ("shard-1".equals(targetShard)) {
                shard1Jdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", tenantId, "ACTIVE");
            } else {
                shard2Jdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", tenantId, "ACTIVE");
            }
        }

        MigrationDeltaCalculator calculator = new MigrationDeltaCalculator(primaryDs, props);
        Map<String, DataSource> shards = Map.of("shard-1", shard1Ds, "shard-2", shard2Ds);

        List<MigrationDeltaCalculator.MigrationAction> deltas = calculator.calculatePrimaryMisalignmentDelta(
                router, shards, FractalProperties.DrainCheckMode.COUNT_THEN_PROBE, 200);

        // Instant Count Guard matches! (1000 == 1000)
        assertThat(deltas).isEmpty();
    }

    @Test
    void tier2StatusProbeShouldQuicklyReturnOnlyUnsyncedTenants() {
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        JdbcTemplate shard1Jdbc = new JdbcTemplate(shard1Ds);
        JdbcTemplate shard2Jdbc = new JdbcTemplate(shard2Ds);

        // 500 aligned tenants
        for (int i = 0; i < 500; i++) {
            String tenantId = "tenant-" + String.format("%05d", i);
            primaryJdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", tenantId, "ACTIVE");
            String target = router.routeNode(tenantId);
            if ("shard-1".equals(target)) {
                shard1Jdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", tenantId, "ACTIVE");
            } else {
                shard2Jdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", tenantId, "ACTIVE");
            }
        }

        // Add 3 new unaligned tenants with PENDING status on primary only
        primaryJdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", "new-tenant-1", "PENDING");
        primaryJdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", "new-tenant-2", "NEW");
        primaryJdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", "new-tenant-3", null);

        MigrationDeltaCalculator calculator = new MigrationDeltaCalculator(primaryDs, props);
        Map<String, DataSource> shards = Map.of("shard-1", shard1Ds, "shard-2", shard2Ds);

        List<MigrationDeltaCalculator.MigrationAction> deltas = calculator.calculatePrimaryMisalignmentDelta(
                router, shards, FractalProperties.DrainCheckMode.COUNT_THEN_PROBE, 200);

        assertThat(deltas).hasSize(3);
        List<String> ids = deltas.stream().map(MigrationDeltaCalculator.MigrationAction::userId).toList();
        assertThat(ids).containsExactlyInAnyOrder("new-tenant-1", "new-tenant-2", "new-tenant-3");
    }

    @Test
    void tier3KeysetStreamingShouldStreamChunksWithConstantMemoryAndFindDiscrepancies() {
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        JdbcTemplate shard1Jdbc = new JdbcTemplate(shard1Ds);
        JdbcTemplate shard2Jdbc = new JdbcTemplate(shard2Ds);

        // Seed 3,000 tenants on primary, but only put 2,995 onto shards (all status ACTIVE)
        Set<String> missingTenants = Set.of("tenant-00100", "tenant-00500", "tenant-01200", "tenant-02100", "tenant-02800");

        for (int i = 0; i < 3000; i++) {
            String tenantId = "tenant-" + String.format("%05d", i);
            primaryJdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", tenantId, "ACTIVE");
            if (!missingTenants.contains(tenantId)) {
                String target = router.routeNode(tenantId);
                if ("shard-1".equals(target)) {
                    shard1Jdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", tenantId, "ACTIVE");
                } else {
                    shard2Jdbc.update("INSERT INTO users (id, status) VALUES (?, ?)", tenantId, "ACTIVE");
                }
            }
        }

        MigrationDeltaCalculator calculator = new MigrationDeltaCalculator(primaryDs, props);
        Map<String, DataSource> shards = Map.of("shard-1", shard1Ds, "shard-2", shard2Ds);

        // Force Tier 3 via FULL_STREAMING with small batchSize=250 to force 12 sequential keyset chunks
        List<MigrationDeltaCalculator.MigrationAction> deltas = calculator.calculatePrimaryMisalignmentDelta(
                router, shards, FractalProperties.DrainCheckMode.FULL_STREAMING, 250);

        assertThat(deltas).hasSize(5);
        List<String> detectedIds = deltas.stream().map(MigrationDeltaCalculator.MigrationAction::userId).toList();
        assertThat(detectedIds).containsExactlyInAnyOrderElementsOf(missingTenants);
    }
}
