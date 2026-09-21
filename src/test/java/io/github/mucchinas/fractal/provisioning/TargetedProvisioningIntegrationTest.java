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
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TargetedProvisioningIntegrationTest.TestApp.class, properties = {
        "fractal.sharding.rebalancer.enabled=true",
        "fractal.sharding.rebalancer.root-table=users",
        "fractal.sharding.rebalancer.root-id-column=id",
        "fractal.sharding.rebalancer.status-column=sync_status",
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:target_prov_primary",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",
        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:target_prov_shard1",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",
        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:target_prov_shard2",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class TargetedProvisioningIntegrationTest {

    @Autowired
    private OnboardingService onboardingService;

    @Autowired
    private ConsistentHashRouter router;

    @Autowired
    private io.github.mucchinas.fractal.rebalance.EntityTableMetadataResolver metadataResolver;

    private DriverManagerDataSource primaryDs;
    private DriverManagerDataSource shard1Ds;
    private DriverManagerDataSource shard2Ds;

    @BeforeEach
    void setUp() {
        metadataResolver.setResolvedRootClass(TestUser.class);
        primaryDs = createDataSource("target_prov_primary");
        shard1Ds = createDataSource("target_prov_shard1");
        shard2Ds = createDataSource("target_prov_shard2");

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
        jdbc.execute("CREATE TABLE IF NOT EXISTS users (" +
                "id VARCHAR(255) PRIMARY KEY, " +
                "email VARCHAR(255), " +
                "user_name VARCHAR(255), " +
                "sync_status VARCHAR(50))");
    }

    private void dropSchema(DriverManagerDataSource ds) {
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        try { jdbc.execute("DROP TABLE IF EXISTS users"); } catch (Exception ignored) {}
    }

    @Test
    void shouldJitProvisionNewUserOnPrimaryAndTargetShard() {
        String userId = "user-kc-101";
        Jwt jwt = Jwt.withTokenValue("mock-token")
                .header("alg", "none")
                .claim("sub", userId)
                .claim("email", "kc101@acme.com")
                .claim("preferred_username", "kc101")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(3600))
                .build();

        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt));

        // Call method with @Sharded(provision = true)
        String routedShard = onboardingService.onboardUser(userId);
        assertThat(routedShard).isNotBlank();

        // Check primary database
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        Map<String, Object> primaryRow = primaryJdbc.queryForMap("SELECT * FROM users WHERE id = ?", userId);
        assertThat(primaryRow.get("id")).isEqualTo(userId);
        assertThat(primaryRow.get("email")).isEqualTo("kc101@acme.com");
        assertThat(primaryRow.get("user_name")).isEqualTo("kc101");
        assertThat(primaryRow.get("sync_status")).isEqualTo("ACTIVE");

        // Check target shard
        String expectedShard = router.routeNode(userId);
        assertThat(routedShard).isEqualTo(expectedShard);

        JdbcTemplate shardJdbc = new JdbcTemplate("shard-1".equals(expectedShard) ? shard1Ds : shard2Ds);
        Map<String, Object> shardRow = shardJdbc.queryForMap("SELECT * FROM users WHERE id = ?", userId);
        assertThat(shardRow.get("id")).isEqualTo(userId);
        assertThat(shardRow.get("email")).isEqualTo("kc101@acme.com");
        assertThat(shardRow.get("user_name")).isEqualTo("kc101");
        assertThat(shardRow.get("sync_status")).isEqualTo("ACTIVE");
    }

    @Entity
    @Table(name = "users")
    @ShardedRoot
    public static class TestUser {
        @Id
        @ShardedKey
        private String id;

        @Column(name = "email")
        private String email;

        @Column(name = "user_name")
        private String username;

        @ShardedStatus
        @Column(name = "sync_status")
        private String syncStatus;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getSyncStatus() { return syncStatus; }
        public void setSyncStatus(String syncStatus) { this.syncStatus = syncStatus; }
    }

    @Service
    public static class OnboardingService {
        @Sharded(key = "#userId", provision = true)
        public String onboardUser(String userId) {
            return ShardContextHolder.getShard();
        }
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({FractalAutoConfiguration.class, OnboardingService.class})
    public static class TestApp {}
}
