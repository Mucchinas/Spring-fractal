package io.github.mucchinas.fractal.rebalance;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mucchinas.fractal.annotation.ShardedEntity;
import io.github.mucchinas.fractal.annotation.ShardedKey;
import io.github.mucchinas.fractal.annotation.ShardedStatus;
import io.github.mucchinas.fractal.config.FractalProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EntityRebalanceIntegrationTest {

    private HikariDataSource primaryDs;
    private HikariDataSource shard1Ds;
    private HikariDataSource shard2Ds;

    private JdbcTemplate shard1Jdbc;
    private JdbcTemplate shard2Jdbc;

    // Domain entities with multi-hop hierarchy
    @ShardedEntity(table = "organizations", root = true)
    static class OrgEntity {
        @ShardedKey(column = "org_id")
        private String orgId;
        private String name;
        @ShardedStatus(column = "sync_status")
        private String syncStatus;
    }

    @ShardedEntity(table = "projects")
    static class ProjectEntity {
        private String id;
        private String name;
        @ShardedKey(targetEntity = OrgEntity.class, column = "org_id")
        private String orgId;
    }

    @ShardedEntity(table = "tasks")
    static class TaskEntity {
        private String id;
        private String title;
        @ShardedKey(targetEntity = ProjectEntity.class, column = "project_id")
        private String projectId;
    }

    @BeforeEach
    void setUp() {
        primaryDs = createDataSource("jdbc:h2:mem:entity_rebal_primary");
        shard1Ds = createDataSource("jdbc:h2:mem:entity_rebal_shard1");
        shard2Ds = createDataSource("jdbc:h2:mem:entity_rebal_shard2");

        shard1Jdbc = new JdbcTemplate(shard1Ds);
        shard2Jdbc = new JdbcTemplate(shard2Ds);

        // Note: NO physical database foreign key constraints are defined in the schema!
        // The dependency hierarchy is discovered solely from @ShardedEntity and @ShardedKey.
        String schema = """
            CREATE TABLE organizations (org_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), sync_status VARCHAR(20));
            CREATE TABLE projects (id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), org_id VARCHAR(50));
            CREATE TABLE tasks (id VARCHAR(50) PRIMARY KEY, title VARCHAR(100), project_id VARCHAR(50));
        """;

        new JdbcTemplate(primaryDs).execute("DROP ALL OBJECTS");
        shard1Jdbc.execute("DROP ALL OBJECTS");
        shard2Jdbc.execute("DROP ALL OBJECTS");

        new JdbcTemplate(primaryDs).execute(schema);
        shard1Jdbc.execute(schema);
        shard2Jdbc.execute(schema);
    }

    @AfterEach
    void tearDown() {
        if (primaryDs != null) primaryDs.close();
        if (shard1Ds != null) shard1Ds.close();
        if (shard2Ds != null) shard2Ds.close();
    }

    @Test
    void shouldRebalanceMultiHopEntitiesWithoutPhysicalDatabaseForeignKeys() {
        // Seed test data on shard1
        shard1Jdbc.update("INSERT INTO organizations (org_id, name, sync_status) VALUES ('org-42', 'Acme Corp', 'ACTIVE')");
        shard1Jdbc.update("INSERT INTO projects (id, name, org_id) VALUES ('proj-1', 'Fractal Project', 'org-42')");
        shard1Jdbc.update("INSERT INTO tasks (id, title, project_id) VALUES ('task-1', 'Build Entity Auto Discovery', 'proj-1')");
        shard1Jdbc.update("INSERT INTO tasks (id, title, project_id) VALUES ('task-2', 'Add Tests', 'proj-1')");

        // Resolve metadata from annotations
        EntityTableMetadataResolver metadataResolver = new EntityTableMetadataResolver();
        EntityMetadataResult metadata = metadataResolver.resolveFromClasses(
                List.of(OrgEntity.class, ProjectEntity.class, TaskEntity.class)
        );

        assertEquals("organizations", metadata.rootTable());
        assertEquals("org_id", metadata.rootIdColumn());
        assertEquals("sync_status", metadata.statusColumn());
        assertEquals(List.of("organizations", "projects", "tasks"), metadata.shardedTables());

        // Configure TableDependencyResolver with entity-resolved foreign keys
        TableDependencyResolver dependencyResolver = new TableDependencyResolver(primaryDs, metadata.foreignKeys());

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setRootTable(metadata.rootTable());
        properties.getRebalancer().setRootIdColumn(metadata.rootIdColumn());
        properties.getRebalancer().setStatusColumn(metadata.statusColumn());
        properties.getRebalancer().setShardedTables(metadata.shardedTables());

        FractalProperties.DataSourceProperties shard1Props = new FractalProperties.DataSourceProperties();
        shard1Props.setJdbcUrl("jdbc:h2:mem:entity_rebal_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Props.setUsername("sa");
        shard1Props.setPassword("");

        FractalProperties.DataSourceProperties shard2Props = new FractalProperties.DataSourceProperties();
        shard2Props.setJdbcUrl("jdbc:h2:mem:entity_rebal_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Props.setUsername("sa");
        shard2Props.setPassword("");

        properties.setShards(Map.of("shard-1", shard1Props, "shard-2", shard2Props));

        TopologyManager topologyManager = new TopologyManager(primaryDs);
        topologyManager.initializeSchema();

        RebalanceEngine engine = new RebalanceEngine(primaryDs, dependencyResolver, topologyManager, properties);

        // Execute migration action
        MigrationDeltaCalculator.MigrationAction action =
                new MigrationDeltaCalculator.MigrationAction("org-42", "shard-1", "shard-2");

        engine.executeMigration(List.of(action));

        // Assert all rows migrated to shard-2 via multi-hop plans
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM organizations WHERE org_id = 'org-42'", Integer.class));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE org_id = 'org-42'", Integer.class));
        assertEquals(2, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE project_id = 'proj-1'", Integer.class));

        // Assert rows pruned from shard-1
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM organizations WHERE org_id = 'org-42'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE org_id = 'org-42'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE project_id = 'proj-1'", Integer.class));
    }

    @Test
    void shouldRebalanceAllTablesWhenShardAllIsTrue() {
        String dropSql = "DROP TABLE IF EXISTS tasks; DROP TABLE IF EXISTS projects; DROP TABLE IF EXISTS organizations; DROP TABLE IF EXISTS notes;";
        new JdbcTemplate(primaryDs).execute(dropSql);
        shard1Jdbc.execute(dropSql);
        shard2Jdbc.execute(dropSql);

        String schemaWithFks = """
            CREATE TABLE organizations (org_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), sync_status VARCHAR(20));
            CREATE TABLE projects (id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), org_id VARCHAR(50), FOREIGN KEY (org_id) REFERENCES organizations(org_id));
            CREATE TABLE tasks (id VARCHAR(50) PRIMARY KEY, title VARCHAR(100), project_id VARCHAR(50), FOREIGN KEY (project_id) REFERENCES projects(id));
            CREATE TABLE notes (id VARCHAR(50) PRIMARY KEY, content VARCHAR(100), org_id VARCHAR(50), FOREIGN KEY (org_id) REFERENCES organizations(org_id));
        """;
        new JdbcTemplate(primaryDs).execute(schemaWithFks);
        shard1Jdbc.execute(schemaWithFks);
        shard2Jdbc.execute(schemaWithFks);

        shard1Jdbc.update("INSERT INTO organizations (org_id, name, sync_status) VALUES ('org-99', 'Global Corp', 'ACTIVE')");
        shard1Jdbc.update("INSERT INTO projects (id, name, org_id) VALUES ('proj-99', 'Project 99', 'org-99')");
        shard1Jdbc.update("INSERT INTO tasks (id, title, project_id) VALUES ('task-99', 'Task 99', 'proj-99')");
        shard1Jdbc.update("INSERT INTO notes (id, content, org_id) VALUES ('note-1', 'Important note', 'org-99')");

        TableDependencyResolver dependencyResolver = new TableDependencyResolver(primaryDs);

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setRootTable("organizations");
        properties.getRebalancer().setRootIdColumn("org_id");
        properties.getRebalancer().setStatusColumn("sync_status");
        properties.getRebalancer().setShardAll(true);

        FractalProperties.DataSourceProperties shard1Props = new FractalProperties.DataSourceProperties();
        shard1Props.setJdbcUrl("jdbc:h2:mem:entity_rebal_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Props.setUsername("sa");
        shard1Props.setPassword("");

        FractalProperties.DataSourceProperties shard2Props = new FractalProperties.DataSourceProperties();
        shard2Props.setJdbcUrl("jdbc:h2:mem:entity_rebal_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Props.setUsername("sa");
        shard2Props.setPassword("");

        properties.setShards(Map.of("shard-1", shard1Props, "shard-2", shard2Props));

        TopologyManager topologyManager = new TopologyManager(primaryDs);
        topologyManager.initializeSchema();

        RebalanceEngine engine = new RebalanceEngine(primaryDs, dependencyResolver, topologyManager, properties);

        MigrationDeltaCalculator.MigrationAction action =
                new MigrationDeltaCalculator.MigrationAction("org-99", "shard-1", "shard-2");

        engine.executeMigration(List.of(action));

        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM organizations WHERE org_id = 'org-99'", Integer.class));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE org_id = 'org-99'", Integer.class));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE project_id = 'proj-99'", Integer.class));
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM notes WHERE org_id = 'org-99'", Integer.class));

        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM organizations WHERE org_id = 'org-99'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM projects WHERE org_id = 'org-99'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM tasks WHERE project_id = 'proj-99'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM notes WHERE org_id = 'org-99'", Integer.class));
    }

    @Test
    void shouldPreserveReplicatedTablesOnAllShardsDuringTenantMigration() {
        String dropSql = "DROP TABLE IF EXISTS currencies; DROP TABLE IF EXISTS tasks; DROP TABLE IF EXISTS projects; DROP TABLE IF EXISTS organizations;";
        new JdbcTemplate(primaryDs).execute(dropSql);
        shard1Jdbc.execute(dropSql);
        shard2Jdbc.execute(dropSql);

        String schema = """
            CREATE TABLE organizations (org_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), sync_status VARCHAR(20));
            CREATE TABLE currencies (code VARCHAR(10) PRIMARY KEY, rate DECIMAL(10, 4));
        """;
        new JdbcTemplate(primaryDs).execute(schema);
        shard1Jdbc.execute(schema);
        shard2Jdbc.execute(schema);

        // Seed currencies on primary and synchronize to both shards
        new JdbcTemplate(primaryDs).update("INSERT INTO currencies (code, rate) VALUES ('EUR', 1.0)");
        new JdbcTemplate(primaryDs).update("INSERT INTO currencies (code, rate) VALUES ('USD', 1.08)");

        ReplicaTableSynchronizer synchronizer = new ReplicaTableSynchronizer(primaryDs, Map.of("shard-1", shard1Ds, "shard-2", shard2Ds));
        synchronizer.syncAllReplicaTables(List.of("currencies"));

        // Seed tenant data on shard1
        shard1Jdbc.update("INSERT INTO organizations (org_id, name, sync_status) VALUES ('org-55', 'Acme Global', 'ACTIVE')");

        TableDependencyResolver dependencyResolver = new TableDependencyResolver(primaryDs);

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setRootTable("organizations");
        properties.getRebalancer().setRootIdColumn("org_id");
        properties.getRebalancer().setStatusColumn("sync_status");
        properties.getRebalancer().setReplicaTables(List.of("currencies"));

        FractalProperties.DataSourceProperties shard1Props = new FractalProperties.DataSourceProperties();
        shard1Props.setJdbcUrl("jdbc:h2:mem:entity_rebal_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Props.setUsername("sa");
        shard1Props.setPassword("");

        FractalProperties.DataSourceProperties shard2Props = new FractalProperties.DataSourceProperties();
        shard2Props.setJdbcUrl("jdbc:h2:mem:entity_rebal_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Props.setUsername("sa");
        shard2Props.setPassword("");

        properties.setShards(Map.of("shard-1", shard1Props, "shard-2", shard2Props));

        TopologyManager topologyManager = new TopologyManager(primaryDs);
        topologyManager.initializeSchema();

        RebalanceEngine engine = new RebalanceEngine(primaryDs, dependencyResolver, topologyManager, properties);

        MigrationDeltaCalculator.MigrationAction action =
                new MigrationDeltaCalculator.MigrationAction("org-55", "shard-1", "shard-2");

        engine.executeMigration(List.of(action));

        // Tenant migrated to shard-2 and pruned from shard-1
        assertEquals(1, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM organizations WHERE org_id = 'org-55'", Integer.class));
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM organizations WHERE org_id = 'org-55'", Integer.class));

        // Replicated reference table is PRESERVED on BOTH shards (never deleted during tenant prune)
        assertEquals(2, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM currencies", Integer.class));
        assertEquals(2, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM currencies", Integer.class));
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
