package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class IncrementalTenantDrainTest {

    private DriverManagerDataSource primaryDs;
    private DriverManagerDataSource shard1Ds;
    private DriverManagerDataSource shard2Ds;
    private ConsistentHashRouter router;
    private FractalProperties.RebalancerProperties props;
    private RebalanceEngine engine;
    private TopologyManager topologyManager;

    @BeforeEach
    void setUp() {
        primaryDs = createDataSource("inc_primary");
        shard1Ds = createDataSource("inc_shard1");
        shard2Ds = createDataSource("inc_shard2");

        createSchema(primaryDs);
        createSchema(shard1Ds);
        createSchema(shard2Ds);

        router = new ConsistentHashRouter(Set.of("shard-1", "shard-2"), 150);

        props = new FractalProperties.RebalancerProperties();
        props.setRootTable("users");
        props.setRootIdColumn("id");
        props.setStatusColumn("status");
        props.setActiveValue("ACTIVE");
        props.setShardedTables(List.of("users", "orders"));

        topologyManager = new TopologyManager(primaryDs, true);
        TableDependencyResolver resolver = new TableDependencyResolver(primaryDs, List.of(
                new TableForeignKey("orders", "user_id", "users", "id")
        ));

        engine = new RebalanceEngine(primaryDs, Map.of("shard-1", shard1Ds, "shard-2", shard2Ds),
                resolver, topologyManager, props);
        engine.setRouter(router);
    }

    @AfterEach
    void tearDown() {
        if (engine != null) engine.close();
        dropTables(primaryDs);
        dropTables(shard1Ds);
        dropTables(shard2Ds);
    }

    private DriverManagerDataSource createDataSource(String name) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl("jdbc:h2:mem:" + name + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        ds.setUsername("sa");
        ds.setPassword("");
        return ds;
    }

    private void createSchema(DataSource ds) {
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        jdbc.execute("CREATE TABLE users (id VARCHAR(255) PRIMARY KEY, status VARCHAR(50))");
        jdbc.execute("CREATE TABLE orders (id VARCHAR(255) PRIMARY KEY, user_id VARCHAR(255), amount DOUBLE PRECISION)");
    }

    private void dropTables(DataSource ds) {
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        try { jdbc.execute("DROP TABLE IF EXISTS orders"); } catch (Exception ignored) {}
        try { jdbc.execute("DROP TABLE IF EXISTS users"); } catch (Exception ignored) {}
    }

    @Test
    void continuousDrainShouldEvacuateOnlyNewTenantsAndPreservePrimaryRoot() {
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        JdbcTemplate shard1Jdbc = new JdbcTemplate(shard1Ds);
        JdbcTemplate shard2Jdbc = new JdbcTemplate(shard2Ds);

        // Pre-existing 5 tenants already aligned on primary and physical shards
        for (int i = 1; i <= 5; i++) {
            String id = "user-" + i;
            primaryJdbc.update("INSERT INTO users (id, status) VALUES (?, 'ACTIVE')", id);
            String shard = router.routeNode(id);
            JdbcTemplate target = "shard-1".equals(shard) ? shard1Jdbc : shard2Jdbc;
            target.update("INSERT INTO users (id, status) VALUES (?, 'ACTIVE')", id);
            target.update("INSERT INTO orders (id, user_id, amount) VALUES (?, ?, ?)", "ord-" + i, id, 100.0 * i);
        }

        // Add 2 new tenants with child data to primary (e.g. provisioned externally)
        primaryJdbc.update("INSERT INTO users (id, status) VALUES (?, 'ACTIVE')", "new-user-1");
        primaryJdbc.update("INSERT INTO orders (id, user_id, amount) VALUES ('ord-new-1', 'new-user-1', 450.0)");

        primaryJdbc.update("INSERT INTO users (id, status) VALUES (?, 'ACTIVE')", "new-user-2");
        primaryJdbc.update("INSERT INTO orders (id, user_id, amount) VALUES ('ord-new-2', 'new-user-2', 750.0)");

        // Run reconciliation
        MigrationDeltaCalculator calculator = new MigrationDeltaCalculator(primaryDs, props);
        Map<String, DataSource> shards = Map.of("shard-1", shard1Ds, "shard-2", shard2Ds);

        List<MigrationDeltaCalculator.MigrationAction> deltas = calculator.calculatePrimaryMisalignmentDelta(
                router, shards, FractalProperties.DrainCheckMode.COUNT_THEN_PROBE, 500);

        assertThat(deltas).hasSize(2);
        assertThat(deltas.stream().map(MigrationDeltaCalculator.MigrationAction::userId).toList())
                .containsExactlyInAnyOrder("new-user-1", "new-user-2");

        // Execute migration
        engine.executeMigration(deltas);

        // Assert: New tenants copied to target shard
        for (String newId : List.of("new-user-1", "new-user-2")) {
            String target = router.routeNode(newId);
            JdbcTemplate targetJdbc = "shard-1".equals(target) ? shard1Jdbc : shard2Jdbc;
            Integer userCount = targetJdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = ?", Integer.class, newId);
            Integer orderCount = targetJdbc.queryForObject("SELECT COUNT(*) FROM orders WHERE user_id = ?", Integer.class, newId);
            assertThat(userCount).isEqualTo(1);
            assertThat(orderCount).isEqualTo(1);

            // Assert: Root record preserved on primary database!
            Integer primaryUserCount = primaryJdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = ?", Integer.class, newId);
            assertThat(primaryUserCount).isEqualTo(1);

            // Assert: Child records deleted from primary
            Integer primaryOrderCount = primaryJdbc.queryForObject("SELECT COUNT(*) FROM orders WHERE user_id = ?", Integer.class, newId);
            assertThat(primaryOrderCount).isEqualTo(0);
        }
    }
}
