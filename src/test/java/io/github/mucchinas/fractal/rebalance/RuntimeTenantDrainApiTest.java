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

class RuntimeTenantDrainApiTest {

    private DriverManagerDataSource primaryDs;
    private DriverManagerDataSource shard1Ds;
    private DriverManagerDataSource shard2Ds;
    private ConsistentHashRouter router;
    private RebalanceEngine engine;

    @BeforeEach
    void setUp() {
        primaryDs = createDataSource("api_primary");
        shard1Ds = createDataSource("api_shard1");
        shard2Ds = createDataSource("api_shard2");

        createSchema(primaryDs);
        createSchema(shard1Ds);
        createSchema(shard2Ds);

        router = new ConsistentHashRouter(Set.of("shard-1", "shard-2"), 150);

        FractalProperties.RebalancerProperties props = new FractalProperties.RebalancerProperties();
        props.setRootTable("users");
        props.setRootIdColumn("id");
        props.setStatusColumn("status");
        props.setActiveValue("ACTIVE");
        props.setShardedTables(List.of("users", "user_profiles"));

        TopologyManager topologyManager = new TopologyManager(primaryDs, true);
        TableDependencyResolver resolver = new TableDependencyResolver(primaryDs, List.of(
                new TableForeignKey("user_profiles", "user_id", "users", "id")
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
        jdbc.execute("CREATE TABLE user_profiles (id VARCHAR(255) PRIMARY KEY, user_id VARCHAR(255), theme VARCHAR(50))");
    }

    private void dropTables(DataSource ds) {
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        try { jdbc.execute("DROP TABLE IF EXISTS user_profiles"); } catch (Exception ignored) {}
        try { jdbc.execute("DROP TABLE IF EXISTS users"); } catch (Exception ignored) {}
    }

    @Test
    void shouldDrainTenantFromPrimaryAtRuntime() {
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        JdbcTemplate shard1Jdbc = new JdbcTemplate(shard1Ds);
        JdbcTemplate shard2Jdbc = new JdbcTemplate(shard2Ds);

        String tenantId = "runtime-user-99";
        String targetShard = router.routeNode(tenantId);

        // Insert new tenant and profile onto primary database
        primaryJdbc.update("INSERT INTO users (id, status) VALUES (?, 'ACTIVE')", tenantId);
        primaryJdbc.update("INSERT INTO user_profiles (id, user_id, theme) VALUES ('p-99', ?, 'dark')", tenantId);

        // Call runtime evacuation API
        engine.drainTenantFromPrimary(tenantId);

        // Check target shard has root and child records
        JdbcTemplate targetJdbc = "shard-1".equals(targetShard) ? shard1Jdbc : shard2Jdbc;
        Integer userCount = targetJdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = ?", Integer.class, tenantId);
        Integer profileCount = targetJdbc.queryForObject("SELECT COUNT(*) FROM user_profiles WHERE user_id = ?", Integer.class, tenantId);
        assertThat(userCount).isEqualTo(1);
        assertThat(profileCount).isEqualTo(1);

        // Check primary database: root record is preserved!
        Integer primaryUserCount = primaryJdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = ?", Integer.class, tenantId);
        assertThat(primaryUserCount).isEqualTo(1);

        // Child record deleted from primary
        Integer primaryProfileCount = primaryJdbc.queryForObject("SELECT COUNT(*) FROM user_profiles WHERE user_id = ?", Integer.class, tenantId);
        assertThat(primaryProfileCount).isEqualTo(0);
    }
}
