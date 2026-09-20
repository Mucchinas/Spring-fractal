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

class PrimaryDrainMigrationTest {

    private DataSource primaryDs;
    private DataSource shard1Ds;
    private DataSource shard2Ds;

    private JdbcTemplate primaryJdbc;
    private JdbcTemplate shard1Jdbc;
    private JdbcTemplate shard2Jdbc;

    private TopologyManager topologyManager;

    @BeforeEach
    void setUp() {
        primaryDs = createDataSource("jdbc:h2:mem:drain_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Ds = createDataSource("jdbc:h2:mem:drain_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Ds = createDataSource("jdbc:h2:mem:drain_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");

        primaryJdbc = new JdbcTemplate(primaryDs);
        shard1Jdbc = new JdbcTemplate(shard1Ds);
        shard2Jdbc = new JdbcTemplate(shard2Ds);

        primaryJdbc.execute("DROP ALL OBJECTS");
        shard1Jdbc.execute("DROP ALL OBJECTS");
        shard2Jdbc.execute("DROP ALL OBJECTS");

        topologyManager = new TopologyManager(primaryDs);
        topologyManager.initializeSchema();
    }

    @Test
    void shouldVerifyPrimaryDrainProperties() {
        FractalProperties.DataSourceProperties props = new FractalProperties.DataSourceProperties();
        assertThat(props.isDrain()).isFalse();

        props.setDrain(true);
        assertThat(props.isDrain()).isTrue();

        props.setDrain(false);
        assertThat(props.isDrain()).isFalse();

        props.setStatus("DRAINING");
        assertThat(props.isDrain()).isTrue();
    }

    @Test
    void shouldManagePrimaryDrainLifecycleInTopologyManagerWithoutPollutingWorkerShards() {
        topologyManager.registerNewShard("shard-1");
        topologyManager.registerNewShard("shard-2");

        assertThat(topologyManager.isPrimaryDrained()).isFalse();

        topologyManager.markPrimaryDraining();
        assertThat(topologyManager.isPrimaryDrained()).isFalse();

        // Worker shard queries must never include "primary"
        assertThat(topologyManager.getKnownShardsFromDb()).containsExactlyInAnyOrder("shard-1", "shard-2");
        assertThat(topologyManager.getActiveShardsFromDb()).containsExactlyInAnyOrder("shard-1", "shard-2");
        assertThat(topologyManager.getDrainingShardsFromDb()).isEmpty();

        topologyManager.recordPendingMigration("tenant-1", TopologyManager.PRIMARY_SHARD_NAME, "shard-1");
        assertThat(topologyManager.hasPendingMigrationsForShard(TopologyManager.PRIMARY_SHARD_NAME)).isTrue();

        topologyManager.completePendingMigration("tenant-1");
        assertThat(topologyManager.hasPendingMigrationsForShard(TopologyManager.PRIMARY_SHARD_NAME)).isFalse();

        topologyManager.markPrimaryDrained();
        assertThat(topologyManager.isPrimaryDrained()).isTrue();

        // Worker shard queries still exclude "primary"
        assertThat(topologyManager.getKnownShardsFromDb()).containsExactlyInAnyOrder("shard-1", "shard-2");
    }

    @Test
    void shouldEvacuateShardedDataFromPrimaryToAssignedShardsAndPreserveRootTable() {
        for (JdbcTemplate jdbc : List.of(primaryJdbc, shard1Jdbc, shard2Jdbc)) {
            jdbc.execute("CREATE TABLE users (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), status VARCHAR(50) NOT NULL)");
            jdbc.execute("CREATE TABLE projects (id VARCHAR(255) PRIMARY KEY, user_id VARCHAR(255), name VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id))");
            jdbc.execute("CREATE TABLE tasks (id VARCHAR(255) PRIMARY KEY, project_id VARCHAR(255), title VARCHAR(255), FOREIGN KEY (project_id) REFERENCES projects(id))");
        }

        primaryJdbc.execute("INSERT INTO users VALUES ('user-1', 'Alice', 'ACTIVE'), ('user-2', 'Bob', 'ACTIVE')");
        primaryJdbc.execute("INSERT INTO projects VALUES ('proj-1', 'user-1', 'Alpha Project'), ('proj-2', 'user-2', 'Beta Project')");
        primaryJdbc.execute("INSERT INTO tasks VALUES ('task-1', 'proj-1', 'Task for Alpha'), ('task-2', 'proj-2', 'Task for Beta')");

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setEnabled(true);
        properties.getRebalancer().setRootTable("users");
        properties.getRebalancer().setRootIdColumn("id");
        properties.getRebalancer().setStatusColumn("status");

        FractalProperties.DataSourceProperties primaryProps = new FractalProperties.DataSourceProperties();
        primaryProps.setJdbcUrl("jdbc:h2:mem:drain_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        primaryProps.setUsername("sa");
        primaryProps.setPassword("");
        primaryProps.setDrain(true);
        properties.setPrimary(primaryProps);

        FractalProperties.DataSourceProperties shard1Props = new FractalProperties.DataSourceProperties();
        shard1Props.setJdbcUrl("jdbc:h2:mem:drain_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Props.setUsername("sa");
        shard1Props.setPassword("");

        FractalProperties.DataSourceProperties shard2Props = new FractalProperties.DataSourceProperties();
        shard2Props.setJdbcUrl("jdbc:h2:mem:drain_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Props.setUsername("sa");
        shard2Props.setPassword("");

        properties.setShards(Map.of(
                "shard-1", shard1Props,
                "shard-2", shard2Props
        ));

        TableDependencyResolver resolver = new TableDependencyResolver(primaryDs);
        RebalanceEngine engine = new RebalanceEngine(primaryDs, resolver, topologyManager, properties);

        topologyManager.markPrimaryDraining();

        MigrationDeltaCalculator.MigrationAction action1 =
                new MigrationDeltaCalculator.MigrationAction("user-1", TopologyManager.PRIMARY_SHARD_NAME, "shard-1");
        MigrationDeltaCalculator.MigrationAction action2 =
                new MigrationDeltaCalculator.MigrationAction("user-2", TopologyManager.PRIMARY_SHARD_NAME, "shard-2");

        engine.executeMigration(List.of(action1, action2));

        // 1. Verify shard-1 has user-1, proj-1, and task-1
        Integer shard1Users = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-1'", Integer.class);
        Integer shard1Projects = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE user_id = 'user-1'", Integer.class);
        Integer shard1Tasks = shard1Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE id = 'task-1'", Integer.class);
        assertThat(shard1Users).isEqualTo(1);
        assertThat(shard1Projects).isEqualTo(1);
        assertThat(shard1Tasks).isEqualTo(1);

        // 2. Verify shard-2 has user-2, proj-2, and task-2
        Integer shard2Users = shard2Jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = 'user-2'", Integer.class);
        Integer shard2Projects = shard2Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE user_id = 'user-2'", Integer.class);
        Integer shard2Tasks = shard2Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE id = 'task-2'", Integer.class);
        assertThat(shard2Users).isEqualTo(1);
        assertThat(shard2Projects).isEqualTo(1);
        assertThat(shard2Tasks).isEqualTo(1);

        // 3. Verify primary has evacuated child tables
        Integer primaryProjects = primaryJdbc.queryForObject("SELECT COUNT(*) FROM projects", Integer.class);
        Integer primaryTasks = primaryJdbc.queryForObject("SELECT COUNT(*) FROM tasks", Integer.class);
        assertThat(primaryProjects).isEqualTo(0);
        assertThat(primaryTasks).isEqualTo(0);

        // 4. Verify primary STILL RETAINS root table with ACTIVE status
        Integer primaryUsers = primaryJdbc.queryForObject("SELECT COUNT(*) FROM users", Integer.class);
        assertThat(primaryUsers).isEqualTo(2);

        String user1Status = primaryJdbc.queryForObject("SELECT status FROM users WHERE id = 'user-1'", String.class);
        String user2Status = primaryJdbc.queryForObject("SELECT status FROM users WHERE id = 'user-2'", String.class);
        assertThat(user1Status).isEqualTo("ACTIVE");
        assertThat(user2Status).isEqualTo("ACTIVE");

        // 5. Verify pending migrations are completely cleared and primary can be marked DRAINED
        assertThat(topologyManager.hasPendingMigrationsForShard(TopologyManager.PRIMARY_SHARD_NAME)).isFalse();
        topologyManager.markPrimaryDrained();
        assertThat(topologyManager.isPrimaryDrained()).isTrue();
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
