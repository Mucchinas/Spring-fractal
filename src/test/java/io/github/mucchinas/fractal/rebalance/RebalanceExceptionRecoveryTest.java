package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.config.FractalProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RebalanceExceptionRecoveryTest {

    private DataSource primaryDs;
    private DataSource shard1Ds;
    private DataSource shard2Ds;

    private JdbcTemplate primaryJdbc;
    private JdbcTemplate shard1Jdbc;
    private JdbcTemplate shard2Jdbc;

    private RebalanceEngine rebalanceEngine;
    private TopologyManager topologyManager;
    private FractalProperties properties;

    @BeforeEach
    void setUp() {
        primaryDs = createDataSource("jdbc:h2:mem:recovery_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Ds = createDataSource("jdbc:h2:mem:recovery_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Ds = createDataSource("jdbc:h2:mem:recovery_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");

        primaryJdbc = new JdbcTemplate(primaryDs);
        shard1Jdbc = new JdbcTemplate(shard1Ds);
        shard2Jdbc = new JdbcTemplate(shard2Ds);

        primaryJdbc.execute("DROP ALL OBJECTS");
        primaryJdbc.execute("""
            CREATE TABLE users (
                id VARCHAR(255) PRIMARY KEY,
                status VARCHAR(50) NOT NULL
            )
        """);
        primaryJdbc.execute("INSERT INTO users VALUES ('user-fail', 'ACTIVE')");

        shard1Jdbc.execute("DROP ALL OBJECTS");
        shard1Jdbc.execute("CREATE TABLE users (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255))");
        shard1Jdbc.execute("INSERT INTO users VALUES ('user-fail', 'Fail User')");

        // Intentionally create incompatible/failing schema on target shard (e.g. NOT NULL column without default)
        shard2Jdbc.execute("DROP ALL OBJECTS");
        shard2Jdbc.execute("CREATE TABLE users (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), non_null_col VARCHAR(10) NOT NULL)");

        properties = new FractalProperties();
        properties.getRebalancer().setEnabled(true);
        properties.getRebalancer().setRootTable("users");
        properties.getRebalancer().setRootIdColumn("id");
        properties.getRebalancer().setStatusColumn("status");

        topologyManager = new TopologyManager(primaryDs);
        topologyManager.initializeSchema();
        TableDependencyResolver resolver = new TableDependencyResolver(shard1Ds);

        rebalanceEngine = new RebalanceEngine(
                primaryDs,
                Map.of("shard-1", shard1Ds, "shard-2", shard2Ds),
                resolver,
                topologyManager,
                properties.getRebalancer()
        );
    }

    @AfterEach
    void tearDown() {
        primaryJdbc.execute("DROP ALL OBJECTS");
        shard1Jdbc.execute("DROP ALL OBJECTS");
        shard2Jdbc.execute("DROP ALL OBJECTS");
    }

    @Test
    void shouldRollbackTenantStatusToActiveWhenMigrationThrowsException() {
        MigrationDeltaCalculator.MigrationAction failingAction =
                new MigrationDeltaCalculator.MigrationAction("user-fail", "shard-1", "shard-2");

        // Execute migration - this will fail on shard2 because of the NOT NULL column
        rebalanceEngine.executeMigration(List.of(failingAction));

        // 1. Primary database status must be rolled back to ACTIVE (not stuck in MIGRATING)
        String status = primaryJdbc.queryForObject("SELECT status FROM users WHERE id = 'user-fail'", String.class);
        assertThat(status).isEqualTo("ACTIVE");

        // 2. TopologyManager in-memory and DB migration status must NOT be migrating
        assertThat(topologyManager.isTenantMigrating("user-fail")).isFalse();

        // 3. Source shard data must remain intact and safe
        Integer sourceCount = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-fail'", Integer.class);
        assertThat(sourceCount).isEqualTo(1);
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
