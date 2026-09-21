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

@SpringBootTest(classes = CustomClaimMappingTest.TestApp.class, properties = {
        "fractal.sharding.rebalancer.enabled=true",
        "fractal.sharding.rebalancer.root-table=users",
        "fractal.sharding.rebalancer.root-id-column=id",
        "fractal.sharding.rebalancer.status-column=status",
        "fractal.sharding.jwt.attribute-claims.org_tier=tier",
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:claim_primary",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",
        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:claim_shard1",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",
        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:claim_shard2",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class CustomClaimMappingTest {

    @Autowired
    private ClaimTestService claimTestService;

    @Autowired
    private ConsistentHashRouter router;

    @Autowired
    private io.github.mucchinas.fractal.rebalance.EntityTableMetadataResolver metadataResolver;

    private DriverManagerDataSource primaryDs;
    private DriverManagerDataSource shard1Ds;
    private DriverManagerDataSource shard2Ds;

    @BeforeEach
    void setUp() {
        metadataResolver.setResolvedRootClass(ClaimMappedUser.class);
        primaryDs = createDataSource("claim_primary");
        shard1Ds = createDataSource("claim_shard1");
        shard2Ds = createDataSource("claim_shard2");

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
        jdbc.execute("CREATE TABLE IF NOT EXISTS users (id VARCHAR(255) PRIMARY KEY, status VARCHAR(50), tier VARCHAR(50))");
    }

    private void dropSchema(DriverManagerDataSource ds) {
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        try { jdbc.execute("DROP TABLE IF EXISTS users"); } catch (Exception ignored) {}
    }

    @Test
    void shouldMapCustomJwtClaimToSpecifiedColumn() {
        String userId = "enterprise-corp-01";
        Jwt jwt = Jwt.withTokenValue("mock-token")
                .header("alg", "none")
                .claim("sub", userId)
                .claim("org_tier", "ENTERPRISE")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(3600))
                .build();

        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt));

        String shard = claimTestService.registerCompany(userId);
        String expectedShard = router.routeNode(userId);
        assertThat(shard).isEqualTo(expectedShard);

        // Verify tier column on primary database
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        Map<String, Object> primaryRow = primaryJdbc.queryForMap("SELECT * FROM users WHERE id = ?", userId);
        assertThat(primaryRow.get("id")).isEqualTo(userId);
        assertThat(primaryRow.get("status")).isEqualTo("ACTIVE");
        assertThat(primaryRow.get("tier")).isEqualTo("ENTERPRISE");

        // Verify tier column on target shard
        JdbcTemplate targetJdbc = new JdbcTemplate("shard-1".equals(expectedShard) ? shard1Ds : shard2Ds);
        Map<String, Object> shardRow = targetJdbc.queryForMap("SELECT * FROM users WHERE id = ?", userId);
        assertThat(shardRow.get("id")).isEqualTo(userId);
        assertThat(shardRow.get("status")).isEqualTo("ACTIVE");
        assertThat(shardRow.get("tier")).isEqualTo("ENTERPRISE");
    }

    @Entity
    @Table(name = "users")
    @ShardedRoot
    public static class ClaimMappedUser {
        @Id
        @ShardedKey
        private String id;

        @ShardedStatus
        @Column(name = "status")
        private String status;

        @Column(name = "tier")
        private String tier;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public String getTier() { return tier; }
        public void setTier(String tier) { this.tier = tier; }
    }

    @Service
    public static class ClaimTestService {
        @Sharded(key = "#userId", provision = true)
        public String registerCompany(String userId) {
            return ShardContextHolder.getShard();
        }
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({FractalAutoConfiguration.class, ClaimTestService.class})
    public static class TestApp {}
}
