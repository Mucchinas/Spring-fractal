package io.github.mucchinas.fractal.provisioning;

import io.github.mucchinas.fractal.annotation.Sharded;
import io.github.mucchinas.fractal.annotation.ShardedKey;
import io.github.mucchinas.fractal.annotation.ShardedRoot;
import io.github.mucchinas.fractal.annotation.ShardedStatus;
import io.github.mucchinas.fractal.config.FractalAutoConfiguration;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TenantInitializerChildTableSeedingTest.TestApp.class, properties = {
        "fractal.sharding.rebalancer.enabled=true",
        "fractal.sharding.rebalancer.root-table=users",
        "fractal.sharding.rebalancer.root-id-column=id",
        "fractal.sharding.rebalancer.status-column=status",
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:init_primary",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",
        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:init_shard1",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",
        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:init_shard2",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class TenantInitializerChildTableSeedingTest {

    @Autowired
    private ProvisionWithSeedingService seedingService;

    @Autowired
    private ConsistentHashRouter router;

    @Autowired
    private io.github.mucchinas.fractal.rebalance.EntityTableMetadataResolver metadataResolver;

    private DriverManagerDataSource primaryDs;
    private DriverManagerDataSource shard1Ds;
    private DriverManagerDataSource shard2Ds;

    @BeforeEach
    void setUp() {
        metadataResolver.setResolvedRootClass(InitializedUser.class);
        primaryDs = createDataSource("init_primary");
        shard1Ds = createDataSource("init_shard1");
        shard2Ds = createDataSource("init_shard2");

        createSchema(primaryDs);
        createSchema(shard1Ds);
        createSchema(shard2Ds);
    }

    @AfterEach
    void tearDown() {
        metadataResolver.setResolvedRootClass(null);
        SecurityContextHolder.clearContext();
        ShardContextHolder.clear();
        dropSchema(primaryDs);
        dropSchema(shard1Ds);
        dropSchema(shard2Ds);
    }

    private DriverManagerDataSource createDataSource(String name) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl("jdbc:h2:mem:" + name + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        ds.setUsername("sa");
        ds.setPassword("");
        return ds;
    }

    private void createSchema(DriverManagerDataSource ds) {
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        jdbc.execute("CREATE TABLE IF NOT EXISTS users (id VARCHAR(255) PRIMARY KEY, email VARCHAR(255), status VARCHAR(50))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS user_settings (id VARCHAR(255) PRIMARY KEY, user_id VARCHAR(255), theme VARCHAR(50))");
    }

    private void dropSchema(DriverManagerDataSource ds) {
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        try { jdbc.execute("DROP TABLE IF EXISTS user_settings"); } catch (Exception ignored) {}
        try { jdbc.execute("DROP TABLE IF EXISTS users"); } catch (Exception ignored) {}
    }

    @Test
    void tenantInitializerShouldSeedChildTablesOnTargetShard() {
        String userId = "user-with-settings";
        Jwt jwt = Jwt.withTokenValue("mock-token")
                .header("alg", "none")
                .claim("sub", userId)
                .claim("email", "settings@acme.com")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(3600))
                .build();

        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt));

        String shard = seedingService.signup(userId);
        String expectedShard = router.routeNode(userId);
        assertThat(shard).isEqualTo(expectedShard);

        // Verify root record exists on target shard
        JdbcTemplate targetShardJdbc = new JdbcTemplate("shard-1".equals(expectedShard) ? shard1Ds : shard2Ds);
        Integer userCount = targetShardJdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = ?", Integer.class, userId);
        assertThat(userCount).isEqualTo(1);

        // Verify child record was seeded by TenantInitializer on the target shard!
        Map<String, Object> setting = targetShardJdbc.queryForMap("SELECT * FROM user_settings WHERE user_id = ?", userId);
        assertThat(setting.get("user_id")).isEqualTo(userId);
        assertThat(setting.get("theme")).isEqualTo("DARK_MODE");

        // Verify child record is NOT on primary
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        Integer primarySettingCount = primaryJdbc.queryForObject("SELECT COUNT(*) FROM user_settings WHERE user_id = ?", Integer.class, userId);
        assertThat(primarySettingCount).isEqualTo(0);
    }

    @Entity
    @Table(name = "users")
    @ShardedRoot
    public static class InitializedUser {
        @Id
        @ShardedKey
        private String id;

        @Column(name = "email")
        private String email;

        @ShardedStatus
        @Column(name = "status")
        private String status;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
    }

    @Service
    public static class ProvisionWithSeedingService {
        @Sharded(key = "#userId", provision = true)
        public String signup(String userId) {
            return ShardContextHolder.getShard();
        }
    }

    @TestConfiguration
    public static class SeedingConfig {
        @Bean
        public TenantInitializer userSettingsInitializer(DataSource dataSource) {
            return (tenantId, targetShard, rootAttributes) -> {
                // When called, ShardContextHolder is automatically bound to targetShard
                JdbcTemplate routingJdbc = new JdbcTemplate(dataSource);
                routingJdbc.update("INSERT INTO user_settings (id, user_id, theme) VALUES (?, ?, ?)",
                        "setting-" + tenantId, tenantId, "DARK_MODE");
            };
        }
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({FractalAutoConfiguration.class, ProvisionWithSeedingService.class, SeedingConfig.class})
    public static class TestApp {}
}
