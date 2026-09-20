package io.github.mucchinas.fractal.rebalance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TableDependencyResolverTest {

    private DataSource dataSource;
    private TableDependencyResolver resolver;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl("jdbc:h2:mem:tdr_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        ds.setUsername("sa");
        ds.setPassword("");
        this.dataSource = ds;

        JdbcTemplate jdbc = new JdbcTemplate(ds);
        jdbc.execute("DROP ALL OBJECTS");
        jdbc.execute("CREATE TABLE users (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255))");
        jdbc.execute("CREATE TABLE projects (id VARCHAR(255) PRIMARY KEY, user_id VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id))");
        jdbc.execute("CREATE TABLE tasks (id VARCHAR(255) PRIMARY KEY, project_id VARCHAR(255), title VARCHAR(255), FOREIGN KEY (project_id) REFERENCES projects(id))");
        jdbc.execute("CREATE TABLE comments (id VARCHAR(255) PRIMARY KEY, task_id VARCHAR(255), body VARCHAR(255), FOREIGN KEY (task_id) REFERENCES tasks(id))");

        this.resolver = new TableDependencyResolver(ds);
    }

    @Test
    void shouldDiscoverAllForeignKeys() {
        List<TableForeignKey> fks = resolver.loadForeignKeys();
        assertThat(fks).hasSize(3);

        assertThat(fks).anyMatch(fk -> fk.childTable().equals("projects") && fk.parentTable().equals("users"));
        assertThat(fks).anyMatch(fk -> fk.childTable().equals("tasks") && fk.parentTable().equals("projects"));
        assertThat(fks).anyMatch(fk -> fk.childTable().equals("comments") && fk.parentTable().equals("tasks"));
    }

    @Test
    void shouldAutoDiscoverAllTablesFromRootTable() {
        List<String> discovered = resolver.discoverShardedTables("users", null, null);
        assertThat(discovered).containsExactlyInAnyOrder("users", "projects", "tasks", "comments");
    }

    @Test
    void shouldRespectExcludeTables() {
        List<String> discovered = resolver.discoverShardedTables("users", null, List.of("comments"));
        assertThat(discovered).containsExactlyInAnyOrder("users", "projects", "tasks");
        assertThat(discovered).doesNotContain("comments");
    }

    @Test
    void shouldResolveTopologicalInsertAndDeleteOrder() {
        List<String> tables = List.of("tasks", "users", "comments", "projects");
        List<String> insertOrder = resolver.resolveInsertOrder(tables);
        assertThat(insertOrder).containsExactly("users", "projects", "tasks", "comments");

        List<String> deleteOrder = resolver.resolveDeleteOrder(tables);
        assertThat(deleteOrder).containsExactly("comments", "tasks", "projects", "users");
    }

    @Test
    void shouldGenerateCascadedSelectAndQueriesUsingForeignKeyHopping() {
        List<TableMigrationPlan> plans = resolver.resolveMigrationPlans("users", "id", null, null);

        assertThat(plans).hasSize(4);
        TableMigrationPlan usersPlan = plans.stream().filter(p -> p.tableName().equals("users")).findFirst().orElseThrow();
        assertThat(usersPlan.selectSql()).isEqualTo("SELECT * FROM users WHERE id = :userId");
        assertThat(usersPlan.deleteSql()).isEqualTo("DELETE FROM users WHERE id = :userId");
        TableMigrationPlan projectsPlan = plans.stream().filter(p -> p.tableName().equals("projects")).findFirst().orElseThrow();
        assertThat(projectsPlan.selectSql()).isEqualTo("SELECT * FROM projects WHERE user_id = :userId");
        assertThat(projectsPlan.deleteSql()).isEqualTo("DELETE FROM projects WHERE user_id = :userId");
        TableMigrationPlan tasksPlan = plans.stream().filter(p -> p.tableName().equals("tasks")).findFirst().orElseThrow();
        assertThat(tasksPlan.selectSql())
                .contains("SELECT tasks.* FROM tasks")
                .contains("JOIN projects ON tasks.project_id = projects.id")
                .contains("WHERE users.id = :userId");
        assertThat(tasksPlan.deleteSql())
                .isEqualTo("DELETE FROM tasks WHERE project_id IN (SELECT id FROM projects WHERE user_id = :userId)");
        TableMigrationPlan commentsPlan = plans.stream().filter(p -> p.tableName().equals("comments")).findFirst().orElseThrow();
        assertThat(commentsPlan.selectSql())
                .contains("SELECT comments.* FROM comments")
                .contains("JOIN tasks ON comments.task_id = tasks.id")
                .contains("JOIN projects ON tasks.project_id = projects.id")
                .contains("WHERE users.id = :userId");
        assertThat(commentsPlan.deleteSql())
                .isEqualTo("DELETE FROM comments WHERE task_id IN (SELECT id FROM tasks WHERE project_id IN (SELECT id FROM projects WHERE user_id = :userId))");
    }

    @Test
    void shouldDiscoverAllDatabaseTablesExcludingFractalAndExcludes() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE audit_logs (id VARCHAR(255) PRIMARY KEY, action VARCHAR(255))");
        jdbc.execute("CREATE TABLE fractal_shard_topology (shard_id VARCHAR(50) PRIMARY KEY)");
        jdbc.execute("CREATE TABLE fractal_locks (lock_name VARCHAR(50) PRIMARY KEY)");
        jdbc.execute("CREATE TABLE fractal_tenant_migrations (id BIGINT PRIMARY KEY)");

        List<String> allTables = resolver.discoverAllDatabaseTables(List.of("audit_logs"));
        assertThat(allTables).contains("users", "projects", "tasks", "comments");
        assertThat(allTables).doesNotContain("audit_logs", "fractal_shard_topology", "fractal_locks", "fractal_tenant_migrations");
    }

    @Test
    void shouldDiscoverShardedTablesWithShardAllSeparatingReplicaTables() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE orders (id VARCHAR(255) PRIMARY KEY, user_id VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id))");
        jdbc.execute("CREATE TABLE currencies (code VARCHAR(255) PRIMARY KEY, rate VARCHAR(255))");

        List<String> replicas = resolver.discoverReplicaTables("users", null, null, true);
        assertThat(replicas).contains("currencies");
        assertThat(replicas).doesNotContain("users", "projects", "tasks", "comments", "orders");

        List<String> sharded = resolver.discoverShardedTables("users", null, replicas, null, true);
        assertThat(sharded.get(0)).isEqualTo("users");
        assertThat(sharded).contains("users", "projects", "tasks", "comments", "orders");
        assertThat(sharded).doesNotContain("currencies");
    }

    @Test
    void shouldResolveMigrationPlansWithShardAll() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE notifications (user_id VARCHAR(255) PRIMARY KEY, msg VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id))");

        List<TableMigrationPlan> plans = resolver.resolveMigrationPlans("users", "id", null, null, true);
        assertThat(plans).anyMatch(p -> p.tableName().equals("users"));
        assertThat(plans).anyMatch(p -> p.tableName().equals("notifications"));
    }
}
