package io.github.mucchinas.fractal.rebalance;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mucchinas.fractal.config.FractalProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class TopologyManagerCaffeineCacheTest {

    private HikariDataSource dataSource;
    private JdbcTemplate jdbcTemplate;
    private TopologyManager topologyManager;
    private FractalProperties.RebalancerProperties rebalProps;

    @BeforeEach
    void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:top_caffeine_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        config.setPassword("");
        dataSource = new HikariDataSource(config);
        jdbcTemplate = new JdbcTemplate(dataSource);
        topologyManager = new TopologyManager(dataSource, true, Duration.ofMillis(300), 1000L);
        topologyManager.initializeSchema();

        rebalProps = new FractalProperties.RebalancerProperties();
        rebalProps.setEnabled(true);
        rebalProps.setRootTable("organizations");
        rebalProps.setRootIdColumn("org_id");
        rebalProps.setStatusColumn("sync_status");
        rebalProps.setMigratingValue("MIGRATING");

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS organizations (
                org_id VARCHAR(64) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                sync_status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE'
            )
        """);
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
    void shouldCacheActiveStatusAndAvoidRepeatedDatabaseQueries() {
        String tenantId = "cached-tenant-1";
        jdbcTemplate.update("INSERT INTO organizations (org_id, name, sync_status) VALUES (?, ?, 'ACTIVE')", tenantId, "Active Corp");
        assertFalse(topologyManager.isTenantMigrating(tenantId, rebalProps));
        assertEquals(Boolean.FALSE, topologyManager.getMigrationStatusCache().getIfPresent(tenantId));
        jdbcTemplate.update("UPDATE organizations SET sync_status = 'MIGRATING' WHERE org_id = ?", tenantId);
        assertFalse(topologyManager.isTenantMigrating(tenantId, rebalProps), "Must hit cache and return FALSE within TTL");
        topologyManager.markTenantMigrating(tenantId);
        assertTrue(topologyManager.isTenantMigrating(tenantId, rebalProps), "Cache was primed with true immediately");

        topologyManager.markTenantActive(tenantId);
        assertFalse(topologyManager.isTenantMigrating(tenantId, rebalProps), "Cache was primed with false immediately");
    }

    @Test
    void shouldExpireCachedEntryAfterTtl() throws InterruptedException {
        String tenantId = "expiring-tenant-2";
        jdbcTemplate.update("INSERT INTO organizations (org_id, name, sync_status) VALUES (?, ?, 'ACTIVE')", tenantId, "Expiring Corp");
        assertFalse(topologyManager.isTenantMigrating(tenantId, rebalProps));
        assertEquals(Boolean.FALSE, topologyManager.getMigrationStatusCache().getIfPresent(tenantId));
        jdbcTemplate.update("UPDATE organizations SET sync_status = 'MIGRATING' WHERE org_id = ?", tenantId);
        assertFalse(topologyManager.isTenantMigrating(tenantId, rebalProps));
        Thread.sleep(350);
        assertTrue(topologyManager.isTenantMigrating(tenantId, rebalProps), "Cache expired; reloaded MIGRATING from Primary DB");
    }
}
