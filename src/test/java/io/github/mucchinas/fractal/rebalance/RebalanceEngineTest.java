package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.config.FractalProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RebalanceEngineTest {

    private DataSource primaryDs;
    private DataSource shard1Ds;
    private DataSource shard2Ds;

    private JdbcTemplate primaryJdbc;
    private JdbcTemplate shard1Jdbc;
    private JdbcTemplate shard2Jdbc;

    private RebalanceEngine rebalanceEngine;

    @BeforeEach
    void setUp() {
        primaryDs = createDataSource("jdbc:h2:mem:rebal_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Ds = createDataSource("jdbc:h2:mem:rebal_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Ds = createDataSource("jdbc:h2:mem:rebal_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");

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
        primaryJdbc.execute("INSERT INTO users VALUES ('user-1', 'ACTIVE'), ('user-2', 'ACTIVE')");
        for (JdbcTemplate shardJdbc : List.of(shard1Jdbc, shard2Jdbc)) {
            shardJdbc.execute("DROP ALL OBJECTS");
            shardJdbc.execute("CREATE TABLE users (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255))");
            shardJdbc.execute("CREATE TABLE projects (id VARCHAR(255) PRIMARY KEY, user_id VARCHAR(255), name VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id))");
            shardJdbc.execute("CREATE TABLE tasks (id VARCHAR(255) PRIMARY KEY, project_id VARCHAR(255), title VARCHAR(255), FOREIGN KEY (project_id) REFERENCES projects(id))");
        }
        shard1Jdbc.execute("INSERT INTO users VALUES ('user-1', 'Alice'), ('user-2', 'Bob')");
        shard1Jdbc.execute("INSERT INTO projects VALUES ('proj-1', 'user-1', 'Alpha'), ('proj-2', 'user-2', 'Beta')");
        shard1Jdbc.execute("INSERT INTO tasks VALUES ('task-1', 'proj-1', 'Task for Alice'), ('task-2', 'proj-2', 'Task for Bob')");
        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setEnabled(true);
        properties.getRebalancer().setRootTable("users");
        properties.getRebalancer().setRootIdColumn("id");
        properties.getRebalancer().setStatusColumn("status");

        FractalProperties.DataSourceProperties shard1Props = new FractalProperties.DataSourceProperties();
        shard1Props.setJdbcUrl("jdbc:h2:mem:rebal_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Props.setUsername("sa");
        shard1Props.setPassword("");

        FractalProperties.DataSourceProperties shard2Props = new FractalProperties.DataSourceProperties();
        shard2Props.setJdbcUrl("jdbc:h2:mem:rebal_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Props.setUsername("sa");
        shard2Props.setPassword("");

        properties.setShards(Map.of("shard-1", shard1Props, "shard-2", shard2Props));
        TableDependencyResolver resolver = new TableDependencyResolver(shard1Ds);
        TopologyManager topologyManager = new TopologyManager(primaryDs);

        rebalanceEngine = new RebalanceEngine(primaryDs, resolver, topologyManager, properties);
    }

    @Test
    void shouldMigrateUserWithHierarchicalForeignKeysFromShard1ToShard2() {
        MigrationDeltaCalculator.MigrationAction action =
                new MigrationDeltaCalculator.MigrationAction("user-1", "shard-1", "shard-2");
        rebalanceEngine.executeMigration(List.of(action));
        Integer targetUsers = shard2Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-1'", Integer.class);
        Integer targetProjects = shard2Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE user_id = 'user-1'", Integer.class);
        Integer targetTasks = shard2Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE id = 'task-1'", Integer.class);

        assertThat(targetUsers).isEqualTo(1);
        assertThat(targetProjects).isEqualTo(1);
        assertThat(targetTasks).isEqualTo(1);
        Integer sourceUsers = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-1'", Integer.class);
        Integer sourceProjects = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE user_id = 'user-1'", Integer.class);
        Integer sourceTasks = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE id = 'task-1'", Integer.class);

        assertThat(sourceUsers).isEqualTo(0);
        assertThat(sourceProjects).isEqualTo(0);
        assertThat(sourceTasks).isEqualTo(0);
        Integer remainingUser2 = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-2'", Integer.class);
        Integer remainingProj2 = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE user_id = 'user-2'", Integer.class);
        Integer remainingTask2 = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE id = 'task-2'", Integer.class);

        assertThat(remainingUser2).isEqualTo(1);
        assertThat(remainingProj2).isEqualTo(1);
        assertThat(remainingTask2).isEqualTo(1);
    }

    @Test
    void shouldSafelyHandleMissingSourceOrTargetShardWithoutLockingTenant() {
        primaryJdbc.execute("INSERT INTO users VALUES ('user-unmapped', 'ACTIVE')");

        MigrationDeltaCalculator.MigrationAction invalidTargetAction =
                new MigrationDeltaCalculator.MigrationAction("user-unmapped", "shard-1", "shard-nonexistent");
        rebalanceEngine.executeMigration(List.of(invalidTargetAction));

        // Tenant status must remain/return to ACTIVE and not stuck in MIGRATING
        String status = primaryJdbc.queryForObject("SELECT status FROM users WHERE id = 'user-unmapped'", String.class);
        assertThat(status).isEqualTo("ACTIVE");

        MigrationDeltaCalculator.MigrationAction invalidSourceAction =
                new MigrationDeltaCalculator.MigrationAction("user-unmapped", "shard-nonexistent", "shard-2");
        rebalanceEngine.executeMigration(List.of(invalidSourceAction));

        status = primaryJdbc.queryForObject("SELECT status FROM users WHERE id = 'user-unmapped'", String.class);
        assertThat(status).isEqualTo("ACTIVE");
    }

    @Test
    void shouldHandleNullOrEmptyMigrationActionsGracefully() {
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> {
            rebalanceEngine.executeMigration(null);
            rebalanceEngine.executeMigration(List.of());
        });
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
