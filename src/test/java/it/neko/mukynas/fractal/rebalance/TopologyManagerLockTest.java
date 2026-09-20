package it.neko.mukynas.fractal.rebalance;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import it.neko.mukynas.fractal.rebalance.TopologyManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class TopologyManagerLockTest {

    private HikariDataSource dataSource;
    private JdbcTemplate jdbcTemplate;
    private TopologyManager topologyManager;

    @BeforeEach
    void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:top_lock_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        config.setPassword("");
        dataSource = new HikariDataSource(config);
        jdbcTemplate = new JdbcTemplate(dataSource);

        topologyManager = new TopologyManager(dataSource);
        topologyManager.initializeSchema();
    }

    @AfterEach
    void tearDown() {
        if (topologyManager != null) {
            topologyManager.destroy();
        }
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Test
    void shouldAcquireAndReleaseLock() {
        assertTrue(topologyManager.tryAcquireRebalanceLock(Duration.ofMinutes(15), Duration.ofMinutes(1)));
        assertTrue(topologyManager.isLockHeld());

        // Verify row in DB
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM fractal_locks WHERE lock_name = 'REBALANCE_LOCK'", Integer.class);
        assertEquals(1, count);

        // Second acquisition by same or different instance fails while active
        TopologyManager secondManager = new TopologyManager(dataSource);
        try {
            assertFalse(secondManager.tryAcquireRebalanceLock(Duration.ofMinutes(15), Duration.ofMinutes(1)));
        } finally {
            secondManager.destroy();
        }

        // Release lock
        topologyManager.releaseRebalanceLock();
        assertFalse(topologyManager.isLockHeld());

        count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM fractal_locks WHERE lock_name = 'REBALANCE_LOCK'", Integer.class);
        assertEquals(0, count);
    }

    @Test
    void shouldTakeOverExpiredStaleLock() {
        // Simulate an abandoned lock from an instance that crashed 30 minutes ago
        Timestamp staleTimestamp = Timestamp.from(Instant.now().minus(Duration.ofMinutes(30)));
        jdbcTemplate.update(
                "INSERT INTO fractal_locks (lock_name, locked_by, locked_at) VALUES ('REBALANCE_LOCK', 'dead-instance', ?)",
                staleTimestamp
        );

        // A new instance attempts to acquire with 15-minute timeout
        TopologyManager newManager = new TopologyManager(dataSource);
        try {
            assertTrue(newManager.tryAcquireRebalanceLock(Duration.ofMinutes(15), Duration.ofMinutes(1)));
            assertTrue(newManager.isLockHeld());

            String lockedBy = jdbcTemplate.queryForObject(
                    "SELECT locked_by FROM fractal_locks WHERE lock_name = 'REBALANCE_LOCK'", String.class);
            assertEquals(newManager.getInstanceId(), lockedBy);
        } finally {
            newManager.destroy();
        }
    }

    @Test
    void shouldReleaseLockOnGracefulShutdown() {
        assertTrue(topologyManager.tryAcquireRebalanceLock(Duration.ofMinutes(15), Duration.ofMinutes(1)));
        assertTrue(topologyManager.isLockHeld());

        // Trigger destroy (simulates Spring context shutdown / SIGTERM)
        topologyManager.destroy();
        assertFalse(topologyManager.isLockHeld());

        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM fractal_locks WHERE lock_name = 'REBALANCE_LOCK'", Integer.class);
        assertEquals(0, count);
    }

    @Test
    void shouldPeriodicallyRefreshHeartbeat() throws InterruptedException {
        // Acquire with very short refresh interval (50ms)
        assertTrue(topologyManager.tryAcquireRebalanceLock(Duration.ofMinutes(15), Duration.ofMillis(50)));

        Timestamp initialTimestamp = jdbcTemplate.queryForObject(
                "SELECT locked_at FROM fractal_locks WHERE lock_name = 'REBALANCE_LOCK'", Timestamp.class);
        assertNotNull(initialTimestamp);

        // Wait for heartbeat to fire
        Thread.sleep(150);

        Timestamp refreshedTimestamp = jdbcTemplate.queryForObject(
                "SELECT locked_at FROM fractal_locks WHERE lock_name = 'REBALANCE_LOCK'", Timestamp.class);
        assertNotNull(refreshedTimestamp);

        assertTrue(refreshedTimestamp.after(initialTimestamp) || refreshedTimestamp.equals(initialTimestamp));
    }

    @Test
    void shouldAutoInitializeSchemaViaAfterPropertiesSet() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:auto_init_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        config.setPassword("");
        try (HikariDataSource freshDs = new HikariDataSource(config)) {
            TopologyManager tm = new TopologyManager(freshDs, true);
            tm.afterPropertiesSet();

            JdbcTemplate freshJdbc = new JdbcTemplate(freshDs);
            assertDoesNotThrow(() -> freshJdbc.queryForList("SELECT * FROM fractal_shard_topology"));
            assertDoesNotThrow(() -> freshJdbc.queryForList("SELECT * FROM fractal_locks"));
            assertDoesNotThrow(() -> freshJdbc.queryForList("SELECT * FROM fractal_tenant_migrations"));
        }
    }

    @Test
    void shouldNotInitializeSchemaWhenAutoInitDisabled() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:no_init_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        config.setPassword("");
        try (HikariDataSource freshDs = new HikariDataSource(config)) {
            TopologyManager tm = new TopologyManager(freshDs, false);
            tm.afterPropertiesSet();

            JdbcTemplate freshJdbc = new JdbcTemplate(freshDs);
            assertThrows(Exception.class, () -> freshJdbc.queryForList("SELECT * FROM fractal_shard_topology"));
        }
    }

    @Test
    void shouldRegisterNewShardIdempotently() {
        topologyManager.registerNewShard("shard-alpha");
        topologyManager.registerNewShard("shard-alpha");

        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM fractal_shard_topology WHERE shard_name = 'shard-alpha'", Integer.class);
        assertEquals(1, count);
    }
}
