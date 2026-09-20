package io.github.mucchinas.fractal.rebalance;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mucchinas.fractal.config.FractalProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RebalanceEngineIdempotencyTest {

    private HikariDataSource primaryDs;
    private HikariDataSource shard1Ds;
    private HikariDataSource shard2Ds;

    private JdbcTemplate primaryJdbc;
    private JdbcTemplate shard1Jdbc;
    private JdbcTemplate shard2Jdbc;

    private TopologyManager topologyManager;
    private RebalanceEngine engine;
    private FractalProperties properties;

    @BeforeEach
    void setUp() {
        primaryDs = createDataSource("jdbc:h2:mem:idem_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Ds = createDataSource("jdbc:h2:mem:idem_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Ds = createDataSource("jdbc:h2:mem:idem_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");

        primaryJdbc = new JdbcTemplate(primaryDs);
        shard1Jdbc = new JdbcTemplate(shard1Ds);
        shard2Jdbc = new JdbcTemplate(shard2Ds);

        primaryJdbc.execute("DROP ALL OBJECTS");
        primaryJdbc.execute("CREATE TABLE users (id VARCHAR(50) PRIMARY KEY, status VARCHAR(20))");
        primaryJdbc.execute("INSERT INTO users VALUES ('user-1', 'ACTIVE')");
        String shardSchema = """
            DROP ALL OBJECTS;
            CREATE TABLE users (id VARCHAR(50) PRIMARY KEY, name VARCHAR(100));
            CREATE TABLE projects (id VARCHAR(50) PRIMARY KEY, user_id VARCHAR(50), name VARCHAR(100));
            CREATE TABLE tasks (id VARCHAR(50) PRIMARY KEY, project_id VARCHAR(50), title VARCHAR(100));
        """;
        shard1Jdbc.execute(shardSchema);
        shard2Jdbc.execute(shardSchema);

        topologyManager = new TopologyManager(primaryDs);
        topologyManager.initializeSchema();

        properties = new FractalProperties();
        properties.getRebalancer().setEnabled(true);
        properties.getRebalancer().setRootTable("users");
        properties.getRebalancer().setRootIdColumn("id");
        properties.getRebalancer().setStatusColumn("status");
        properties.getRebalancer().setShardedTables(List.of("users", "projects", "tasks"));

        FractalProperties.DataSourceProperties s1 = new FractalProperties.DataSourceProperties();
        s1.setJdbcUrl("jdbc:h2:mem:idem_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        s1.setUsername("sa");
        s1.setPassword("");

        FractalProperties.DataSourceProperties s2 = new FractalProperties.DataSourceProperties();
        s2.setJdbcUrl("jdbc:h2:mem:idem_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        s2.setUsername("sa");
        s2.setPassword("");

        properties.setShards(Map.of("shard-1", s1, "shard-2", s2));
        List<TableForeignKey> fks = List.of(
                new TableForeignKey("projects", "user_id", "users", "id"),
                new TableForeignKey("tasks", "project_id", "projects", "id")
        );
        TableDependencyResolver resolver = new TableDependencyResolver(primaryDs, fks);

        engine = new RebalanceEngine(primaryDs, resolver, topologyManager, properties);
    }

    @AfterEach
    void tearDown() {
        if (topologyManager != null) topologyManager.destroy();
        if (primaryDs != null) primaryDs.close();
        if (shard1Ds != null) shard1Ds.close();
        if (shard2Ds != null) shard2Ds.close();
    }

    @Test
    void shouldRecoverWhenInterruptedDuringCopyingPhase() {
        shard1Jdbc.execute("INSERT INTO users VALUES ('user-1', 'Alice')");
        shard1Jdbc.execute("INSERT INTO projects VALUES ('proj-1', 'user-1', 'Alpha')");
        shard1Jdbc.execute("INSERT INTO tasks VALUES ('task-1', 'proj-1', 'First Task')");
        shard2Jdbc.execute("INSERT INTO users VALUES ('user-1', 'Alice')");
        topologyManager.recordMigrationStart("user-1", "shard-1", "shard-2");
        MigrationDeltaCalculator.MigrationAction action =
                new MigrationDeltaCalculator.MigrationAction("user-1", "shard-1", "shard-2");
        engine.executeMigration(List.of(action));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-1'", Integer.class));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE user_id = 'user-1'", Integer.class));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE project_id = 'proj-1'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-1'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE user_id = 'user-1'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE project_id = 'proj-1'", Integer.class));
        assertNull(topologyManager.getMigrationPhase("user-1"));
        assertEquals("ACTIVE", primaryJdbc.queryForObject("SELECT status FROM users WHERE id = 'user-1'", String.class));
    }

    @Test
    void shouldResumeWhenInterruptedDuringPruningPhase() {
        shard2Jdbc.execute("INSERT INTO users VALUES ('user-1', 'Alice')");
        shard2Jdbc.execute("INSERT INTO projects VALUES ('proj-1', 'user-1', 'Alpha')");
        shard2Jdbc.execute("INSERT INTO tasks VALUES ('task-1', 'proj-1', 'First Task')");
        shard1Jdbc.execute("INSERT INTO users VALUES ('user-1', 'Alice')");
        shard1Jdbc.execute("INSERT INTO projects VALUES ('proj-1', 'user-1', 'Alpha')");
        topologyManager.recordMigrationStart("user-1", "shard-1", "shard-2");
        topologyManager.updateMigrationPhase("user-1", TopologyManager.PHASE_PRUNING);
        MigrationDeltaCalculator.MigrationAction action =
                new MigrationDeltaCalculator.MigrationAction("user-1", "shard-1", "shard-2");
        engine.executeMigration(List.of(action));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-1'", Integer.class));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE user_id = 'user-1'", Integer.class));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE project_id = 'proj-1'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-1'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE user_id = 'user-1'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE project_id = 'proj-1'", Integer.class));
        assertNull(topologyManager.getMigrationPhase("user-1"));
        assertEquals("ACTIVE", primaryJdbc.queryForObject("SELECT status FROM users WHERE id = 'user-1'", String.class));
    }

    @Test
    void shouldBeIdempotentWhenExecutedMultipleTimes() {
        shard1Jdbc.execute("INSERT INTO users VALUES ('user-1', 'Alice')");
        shard1Jdbc.execute("INSERT INTO projects VALUES ('proj-1', 'user-1', 'Alpha')");
        shard1Jdbc.execute("INSERT INTO tasks VALUES ('task-1', 'proj-1', 'First Task')");

        MigrationDeltaCalculator.MigrationAction action =
                new MigrationDeltaCalculator.MigrationAction("user-1", "shard-1", "shard-2");
        engine.executeMigration(List.of(action));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-1'", Integer.class));
        engine.executeMigration(List.of(action));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-1'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-1'", Integer.class));
        assertEquals("ACTIVE", primaryJdbc.queryForObject("SELECT status FROM users WHERE id = 'user-1'", String.class));
    }

    private HikariDataSource createDataSource(String url) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(5);
        return new HikariDataSource(config);
    }
}
