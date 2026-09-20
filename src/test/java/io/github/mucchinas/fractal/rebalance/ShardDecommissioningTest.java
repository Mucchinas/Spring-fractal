package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ShardDecommissioningTest {

    private DataSource primaryDs;
    private DataSource shard1Ds;
    private DataSource shard2Ds;
    private DataSource shard3Ds;

    private JdbcTemplate primaryJdbc;
    private JdbcTemplate shard1Jdbc;
    private JdbcTemplate shard2Jdbc;
    private JdbcTemplate shard3Jdbc;

    private TopologyManager topologyManager;

    @BeforeEach
    void setUp() {
        primaryDs = createDataSource("jdbc:h2:mem:decom_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Ds = createDataSource("jdbc:h2:mem:decom_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Ds = createDataSource("jdbc:h2:mem:decom_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard3Ds = createDataSource("jdbc:h2:mem:decom_shard3;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");

        primaryJdbc = new JdbcTemplate(primaryDs);
        shard1Jdbc = new JdbcTemplate(shard1Ds);
        shard2Jdbc = new JdbcTemplate(shard2Ds);
        shard3Jdbc = new JdbcTemplate(shard3Ds);

        primaryJdbc.execute("DROP ALL OBJECTS");
        for (JdbcTemplate shardJdbc : List.of(shard1Jdbc, shard2Jdbc, shard3Jdbc)) {
            shardJdbc.execute("DROP ALL OBJECTS");
        }

        topologyManager = new TopologyManager(primaryDs);
        topologyManager.initializeSchema();
    }

    @Test
    void shouldProperlySegregateActiveAndDecommissioningShardsInProperties() {
        FractalProperties properties = new FractalProperties();

        FractalProperties.DataSourceProperties shard1 = new FractalProperties.DataSourceProperties();
        shard1.setDecommission(false);

        FractalProperties.DataSourceProperties shard2 = new FractalProperties.DataSourceProperties();
        shard2.setDecommission(true);

        FractalProperties.DataSourceProperties shard3 = new FractalProperties.DataSourceProperties();
        shard3.setStatus("DRAINING");

        FractalProperties.DataSourceProperties shard4 = new FractalProperties.DataSourceProperties();
        shard4.setStatus("ACTIVE");

        properties.setShards(Map.of(
                "shard-1", shard1,
                "shard-2", shard2,
                "shard-3", shard3,
                "shard-4", shard4
        ));

        assertThat(properties.getActiveShardNames()).containsExactlyInAnyOrder("shard-1", "shard-4");
        assertThat(properties.getDecommissioningShardNames()).containsExactlyInAnyOrder("shard-2", "shard-3");
    }

    @Test
    void shouldCalculateMigrationDeltaExclusivelyForDecommissionedShardTenants() {
        primaryJdbc.execute("""
            CREATE TABLE accounts (
                id VARCHAR(255) PRIMARY KEY,
                status VARCHAR(50) NOT NULL
            )
        """);

        List<String> dbShards = List.of("shard-1", "shard-2", "shard-3");
        Set<String> activeYamlShards = Set.of("shard-2", "shard-3");

        ConsistentHashRouter initialRouter = new ConsistentHashRouter(dbShards, 150);

        String tenantOnShard1 = null;
        String tenantOnShard2 = null;
        String tenantOnShard3 = null;

        for (int i = 0; i < 2000; i++) {
            String candidate = "user-" + i;
            String node = initialRouter.routeNode(candidate);
            if (tenantOnShard1 == null && "shard-1".equals(node)) {
                tenantOnShard1 = candidate;
            } else if (tenantOnShard2 == null && "shard-2".equals(node)) {
                tenantOnShard2 = candidate;
            } else if (tenantOnShard3 == null && "shard-3".equals(node)) {
                tenantOnShard3 = candidate;
            }
            if (tenantOnShard1 != null && tenantOnShard2 != null && tenantOnShard3 != null) {
                break;
            }
        }

        assertThat(tenantOnShard1).isNotNull();
        assertThat(tenantOnShard2).isNotNull();
        assertThat(tenantOnShard3).isNotNull();

        primaryJdbc.update("INSERT INTO accounts VALUES (?, 'ACTIVE')", tenantOnShard1);
        primaryJdbc.update("INSERT INTO accounts VALUES (?, 'ACTIVE')", tenantOnShard2);
        primaryJdbc.update("INSERT INTO accounts VALUES (?, 'ACTIVE')", tenantOnShard3);

        FractalProperties.RebalancerProperties rebalProps = new FractalProperties.RebalancerProperties();
        rebalProps.setRootTable("accounts");
        rebalProps.setRootIdColumn("id");

        MigrationDeltaCalculator calculator = new MigrationDeltaCalculator(primaryDs, rebalProps);
        List<MigrationDeltaCalculator.MigrationAction> actions = calculator.calculateDelta(dbShards, activeYamlShards, 150);

        assertThat(actions).hasSize(1);
        MigrationDeltaCalculator.MigrationAction action = actions.get(0);
        assertThat(action.userId()).isEqualTo(tenantOnShard1);
        assertThat(action.sourceShard()).isEqualTo("shard-1");
        assertThat(action.targetShard()).isIn("shard-2", "shard-3");
    }

    @Test
    void shouldTrackDecommissioningLifecycleInTopologyManager() {
        topologyManager.registerNewShard("shard-1");
        topologyManager.registerNewShard("shard-2");
        topologyManager.registerNewShard("shard-3");

        assertThat(topologyManager.getKnownShardsFromDb()).containsExactlyInAnyOrder("shard-1", "shard-2", "shard-3");
        assertThat(topologyManager.getActiveShardsFromDb()).containsExactlyInAnyOrder("shard-1", "shard-2", "shard-3");
        assertThat(topologyManager.getDrainingShardsFromDb()).isEmpty();

        topologyManager.markShardDraining("shard-1");

        assertThat(topologyManager.getActiveShardsFromDb()).containsExactlyInAnyOrder("shard-2", "shard-3");
        assertThat(topologyManager.getDrainingShardsFromDb()).containsExactly("shard-1");

        topologyManager.recordPendingMigration("tenant-alpha", "shard-1", "shard-2");
        assertThat(topologyManager.hasPendingMigrationsForShard("shard-1")).isTrue();
        assertThat(topologyManager.hasPendingMigrationsForShard("shard-2")).isFalse();

        topologyManager.completePendingMigration("tenant-alpha");
        assertThat(topologyManager.hasPendingMigrationsForShard("shard-1")).isFalse();

        topologyManager.removeShard("shard-1");
        assertThat(topologyManager.getKnownShardsFromDb()).containsExactlyInAnyOrder("shard-2", "shard-3");
        assertThat(topologyManager.getActiveShardsFromDb()).containsExactlyInAnyOrder("shard-2", "shard-3");
        assertThat(topologyManager.getDrainingShardsFromDb()).isEmpty();
    }

    @Test
    void shouldDrainsDataFromDecommissionedShardToSurvivingShards() {
        primaryJdbc.execute("""
            CREATE TABLE users (
                id VARCHAR(255) PRIMARY KEY,
                status VARCHAR(50) NOT NULL
            )
        """);
        primaryJdbc.execute("INSERT INTO users VALUES ('user-retiring', 'ACTIVE'), ('user-staying', 'ACTIVE')");

        for (JdbcTemplate shardJdbc : List.of(shard1Jdbc, shard2Jdbc, shard3Jdbc)) {
            shardJdbc.execute("CREATE TABLE users (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255))");
            shardJdbc.execute("CREATE TABLE projects (id VARCHAR(255) PRIMARY KEY, user_id VARCHAR(255), name VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id))");
        }

        shard1Jdbc.execute("INSERT INTO users VALUES ('user-retiring', 'Retiring User')");
        shard1Jdbc.execute("INSERT INTO projects VALUES ('proj-1', 'user-retiring', 'Project on Shard 1')");

        shard2Jdbc.execute("INSERT INTO users VALUES ('user-staying', 'Staying User')");
        shard2Jdbc.execute("INSERT INTO projects VALUES ('proj-2', 'user-staying', 'Project on Shard 2')");

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setEnabled(true);
        properties.getRebalancer().setRootTable("users");
        properties.getRebalancer().setRootIdColumn("id");
        properties.getRebalancer().setStatusColumn("status");

        FractalProperties.DataSourceProperties shard1Props = new FractalProperties.DataSourceProperties();
        shard1Props.setJdbcUrl("jdbc:h2:mem:decom_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Props.setUsername("sa");
        shard1Props.setPassword("");
        shard1Props.setDecommission(true);

        FractalProperties.DataSourceProperties shard2Props = new FractalProperties.DataSourceProperties();
        shard2Props.setJdbcUrl("jdbc:h2:mem:decom_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Props.setUsername("sa");
        shard2Props.setPassword("");

        FractalProperties.DataSourceProperties shard3Props = new FractalProperties.DataSourceProperties();
        shard3Props.setJdbcUrl("jdbc:h2:mem:decom_shard3;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard3Props.setUsername("sa");
        shard3Props.setPassword("");

        properties.setShards(Map.of(
                "shard-1", shard1Props,
                "shard-2", shard2Props,
                "shard-3", shard3Props
        ));

        TableDependencyResolver resolver = new TableDependencyResolver(shard1Ds);
        RebalanceEngine engine = new RebalanceEngine(primaryDs, resolver, topologyManager, properties);

        topologyManager.registerNewShard("shard-1");
        topologyManager.registerNewShard("shard-2");
        topologyManager.registerNewShard("shard-3");
        topologyManager.markShardDraining("shard-1");

        MigrationDeltaCalculator.MigrationAction action =
                new MigrationDeltaCalculator.MigrationAction("user-retiring", "shard-1", "shard-3");

        engine.executeMigration(List.of(action));

        Integer targetUsers = shard3Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-retiring'", Integer.class);
        Integer targetProjects = shard3Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE user_id = 'user-retiring'", Integer.class);
        assertThat(targetUsers).isEqualTo(1);
        assertThat(targetProjects).isEqualTo(1);

        Integer sourceUsers = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-retiring'", Integer.class);
        Integer sourceProjects = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE user_id = 'user-retiring'", Integer.class);
        assertThat(sourceUsers).isEqualTo(0);
        assertThat(sourceProjects).isEqualTo(0);

        Integer stayingUser = shard2Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-staying'", Integer.class);
        assertThat(stayingUser).isEqualTo(1);

        assertThat(topologyManager.hasPendingMigrationsForShard("shard-1")).isFalse();
        topologyManager.removeShard("shard-1");

        assertThat(topologyManager.getKnownShardsFromDb()).containsExactlyInAnyOrder("shard-2", "shard-3");
    }

    private DataSource createDataSource(String url) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl(url);
        ds.setUsername("sa");
        ds.setPassword("");
        return ds;
    }
}
