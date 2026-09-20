package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.annotation.Sharded;
import io.github.mucchinas.fractal.config.FractalAutoConfiguration;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import io.github.mucchinas.fractal.exception.TenantMigratingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "fractal.sharding.rebalancer.enabled=true",
        "fractal.sharding.rebalancer.root-table=accounts",
        "fractal.sharding.rebalancer.root-id-column=id",
        "fractal.sharding.rebalancer.status-column=sync_status",
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:dualring_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",
        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:dualring_shard1",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",
        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:dualring_shard2",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password=",
        "fractal.sharding.shards.shard-3.jdbcUrl=jdbc:h2:mem:dualring_shard3",
        "fractal.sharding.shards.shard-3.username=sa",
        "fractal.sharding.shards.shard-3.password="
})
class DualRingMigrationRoutingTest {

    @Autowired
    private ShardedAccountService accountService;

    @Autowired
    private TopologyManager topologyManager;

    @Autowired
    private ConsistentHashRouter router;

    @Autowired
    private DataSource dataSource;

    @Autowired(required = false)
    private org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor fractalRebalanceExecutor;

    @BeforeEach
    void setUp() throws Exception {
        if (fractalRebalanceExecutor != null) {
            long deadline = System.currentTimeMillis() + 3000;
            while ((fractalRebalanceExecutor.getActiveCount() > 0 || fractalRebalanceExecutor.getThreadPoolExecutor().getQueue().size() > 0)
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
        }
        org.springframework.jdbc.datasource.DriverManagerDataSource primaryDs = new org.springframework.jdbc.datasource.DriverManagerDataSource();
        primaryDs.setDriverClassName("org.h2.Driver");
        primaryDs.setUrl("jdbc:h2:mem:dualring_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        primaryDs.setUsername("sa");
        primaryDs.setPassword("");
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);

        topologyManager.initializeSchema();
        primaryJdbc.execute("CREATE TABLE IF NOT EXISTS accounts (id VARCHAR(255) PRIMARY KEY, sync_status VARCHAR(50))");
        primaryJdbc.execute("DELETE FROM fractal_locks");
        primaryJdbc.execute("DELETE FROM fractal_tenant_migrations");

        topologyManager.releaseRebalanceLock();
        topologyManager.clearActiveMigratingTenants();
        topologyManager.clearPendingMigrations();
        topologyManager.setRebalanceActive(false);
        topologyManager.getMigrationStatusCache().invalidateAll();
        topologyManager.getRebalanceActiveCache().invalidateAll();
    }

    @AfterEach
    void tearDown() {
        ShardContextHolder.clear();
        topologyManager.releaseRebalanceLock();
        topologyManager.clearActiveMigratingTenants();
        topologyManager.clearPendingMigrations();
        topologyManager.setRebalanceActive(false);
        topologyManager.getMigrationStatusCache().invalidateAll();
        topologyManager.getRebalanceActiveCache().invalidateAll();
    }

    @Test
    void shouldBypassRebalanceChecksInSteadyState() {
        assertThat(topologyManager.isRebalanceActive()).isFalse();

        // When rebalance is not active, per-tenant status cache is not accessed
        String tenant = "tenant-steady";
        String expectedShard = router.routeNode(tenant);

        String routedShard = accountService.accessAccount(tenant);
        assertThat(routedShard).isEqualTo(expectedShard);

        // Per-tenant migrationStatusCache remains empty because fast path bypassed it
        assertThat(topologyManager.getMigrationStatusCache().asMap()).doesNotContainKey(tenant);
    }

    @Test
    void shouldRoutePendingTenantToSourceShardDuringActiveRebalance() {
        String tenant = "tenant-scheduled";
        String newRingShard = router.routeNode(tenant);
        String oldSourceShard = newRingShard.equals("shard-1") ? "shard-2" : "shard-1";

        // Register pending migration override
        topologyManager.registerPendingMigration(tenant, oldSourceShard);
        assertThat(topologyManager.isRebalanceActive()).isTrue();

        // While pending, requests MUST route to the old source shard
        String routedShard = accountService.accessAccount(tenant);
        assertThat(routedShard).isEqualTo(oldSourceShard);
        assertThat(routedShard).isNotEqualTo(newRingShard);
    }

    @Test
    void shouldThrowTenantMigratingExceptionWhenTenantStartsMigration() {
        String tenant = "tenant-in-flight";
        topologyManager.registerPendingMigration(tenant, "shard-1");
        topologyManager.markTenantMigrating(tenant);

        assertThatThrownBy(() -> accountService.accessAccount(tenant))
                .isInstanceOf(TenantMigratingException.class)
                .hasMessageContaining(tenant);
    }

    @Test
    void shouldCutoverToNewShardImmediatelyWhenMigrationCompletes() {
        String tenant = "tenant-moving";
        String newRingShard = router.routeNode(tenant);
        String oldSourceShard = newRingShard.equals("shard-1") ? "shard-3" : "shard-1";

        // 1. Pending migration -> routes to old source shard
        topologyManager.registerPendingMigration(tenant, oldSourceShard);
        assertThat(accountService.accessAccount(tenant)).isEqualTo(oldSourceShard);

        // 2. Active migration -> throws exception
        topologyManager.markTenantMigrating(tenant);
        assertThatThrownBy(() -> accountService.accessAccount(tenant))
                .isInstanceOf(TenantMigratingException.class);

        // 3. Migration completes -> immediately routes to new ring shard
        topologyManager.completePendingMigration(tenant);
        topologyManager.markTenantActive(tenant);

        String routedShardAfter = accountService.accessAccount(tenant);
        assertThat(routedShardAfter).isEqualTo(newRingShard);
    }

    @Test
    void shouldCacheGlobalRebalanceStatusInCaffeine() {
        // Invalidate and check Caffeine cache
        topologyManager.getRebalanceActiveCache().invalidateAll();
        boolean activeInitial = topologyManager.isRebalanceActive();
        assertThat(activeInitial).isFalse();

        // Check that Caffeine cached the Boolean.FALSE
        Boolean cachedValue = topologyManager.getRebalanceActiveCache().getIfPresent(TopologyManager.GLOBAL_REBALANCE_KEY);
        assertThat(cachedValue).isNotNull();
        assertThat(cachedValue).isFalse();

        // Eager local activation updates Caffeine cache immediately
        topologyManager.setRebalanceActive(true);
        assertThat(topologyManager.getRebalanceActiveCache().getIfPresent(TopologyManager.GLOBAL_REBALANCE_KEY)).isTrue();
        assertThat(topologyManager.isRebalanceActive()).isTrue();

        // Eager local deactivation updates Caffeine cache immediately
        topologyManager.setRebalanceActive(false);
        assertThat(topologyManager.getRebalanceActiveCache().getIfPresent(TopologyManager.GLOBAL_REBALANCE_KEY)).isFalse();
        assertThat(topologyManager.isRebalanceActive()).isFalse();
    }

    @Test
    void shouldNotAffectNonMigratingTenantsDuringRebalance() {
        String migratingTenant = "migrating-user";
        String bystanderTenant = "bystander-user";
        String bystanderExpectedShard = router.routeNode(bystanderTenant);

        topologyManager.registerPendingMigration(migratingTenant, "shard-1");
        topologyManager.markTenantMigrating(migratingTenant);

        // Bystander tenant is NOT moving -> unaffected and routes normally
        String bystanderActualShard = accountService.accessAccount(bystanderTenant);
        assertThat(bystanderActualShard).isEqualTo(bystanderExpectedShard);
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({ShardedAccountService.class, FractalAutoConfiguration.class})
    static class TestApp {}

    @Service
    static class ShardedAccountService {
        @Sharded(key = "#accountId")
        public String accessAccount(String accountId) {
            return ShardContextHolder.getShard();
        }
    }
}
