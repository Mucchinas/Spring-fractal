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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = ParallelKeycloakLoginBurstTest.TestApp.class, properties = {
        "fractal.sharding.rebalancer.enabled=true",
        "fractal.sharding.rebalancer.root-table=users",
        "fractal.sharding.rebalancer.root-id-column=id",
        "fractal.sharding.rebalancer.status-column=status",
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:burst_primary",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",
        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:burst_shard1",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",
        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:burst_shard2",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class ParallelKeycloakLoginBurstTest {

    @Autowired
    private BurstLoginService loginService;

    @Autowired
    private ConsistentHashRouter router;

    @Autowired
    private io.github.mucchinas.fractal.rebalance.EntityTableMetadataResolver metadataResolver;

    private DriverManagerDataSource primaryDs;
    private DriverManagerDataSource shard1Ds;
    private DriverManagerDataSource shard2Ds;

    @BeforeEach
    void setUp() {
        metadataResolver.setResolvedRootClass(BurstUser.class);
        primaryDs = createDataSource("burst_primary");
        shard1Ds = createDataSource("burst_shard1");
        shard2Ds = createDataSource("burst_shard2");

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
    }

    private void dropSchema(DriverManagerDataSource ds) {
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        try { jdbc.execute("DROP TABLE IF EXISTS users"); } catch (Exception ignored) {}
    }

    @Test
    void twentyParallelRequestsForSameUserShouldProvisionCleanlyWithoutErrors() throws Exception {
        int threadCount = 20;
        String userId = "burst-user-888";
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    Jwt jwt = Jwt.withTokenValue("burst-token")
                            .header("alg", "none")
                            .claim("sub", userId)
                            .claim("email", "burst@example.com")
                            .issuedAt(Instant.now())
                            .expiresAt(Instant.now().plusSeconds(3600))
                            .build();

                    SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt));
                    startLatch.await(); // Wait for all threads to be ready

                    String shard = loginService.login(userId);
                    if (shard != null && !shard.isBlank()) {
                        successCount.incrementAndGet();
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    SecurityContextHolder.clearContext();
                    ShardContextHolder.clear();
                    finishLatch.countDown();
                }
            });
        }

        // Fire all 20 threads simultaneously
        startLatch.countDown();
        boolean finished = finishLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(finished).isTrue();
        assertThat(errors).isEmpty();
        assertThat(successCount.get()).isEqualTo(threadCount);

        // Verify exactly 1 record created on primary
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        Integer primaryCount = primaryJdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = ?", Integer.class, userId);
        assertThat(primaryCount).isEqualTo(1);

        // Verify exactly 1 record created on target shard
        String targetShard = router.routeNode(userId);
        JdbcTemplate shardJdbc = new JdbcTemplate("shard-1".equals(targetShard) ? shard1Ds : shard2Ds);
        Integer shardCount = shardJdbc.queryForObject("SELECT COUNT(*) FROM users WHERE id = ?", Integer.class, userId);
        assertThat(shardCount).isEqualTo(1);
    }

    @Entity
    @Table(name = "users")
    @ShardedRoot
    public static class BurstUser {
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
    public static class BurstLoginService {
        @Sharded(key = "#userId", provision = true)
        public String login(String userId) {
            return ShardContextHolder.getShard();
        }
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({FractalAutoConfiguration.class, BurstLoginService.class})
    public static class TestApp {}
}
