package io.github.mucchinas.fractal.aop;

import io.github.mucchinas.fractal.annotation.Sharded;
import io.github.mucchinas.fractal.config.FractalAutoConfiguration;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import io.github.mucchinas.fractal.exception.TenantMigratingException;
import io.github.mucchinas.fractal.rebalance.TopologyManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "fractal.sharding.rebalancer.enabled=true",
        "fractal.sharding.rebalancer.root-table=users",
        "fractal.sharding.rebalancer.root-id-column=id",
        "fractal.sharding.rebalancer.status-column=status",
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:miglock_primary",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",
        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:miglock_shard1",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",
        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:miglock_shard2",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class ShardingAspectMigrationLockTest {

    @Autowired
    private ShardedWorkService workService;

    @Autowired
    private TopologyManager topologyManager;

    @Autowired(required = false)
    private org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor fractalRebalanceExecutor;

    @org.junit.jupiter.api.BeforeAll
    static void initPrimary() {
        org.springframework.jdbc.datasource.DriverManagerDataSource primaryDs = new org.springframework.jdbc.datasource.DriverManagerDataSource();
        primaryDs.setDriverClassName("org.h2.Driver");
        primaryDs.setUrl("jdbc:h2:mem:miglock_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        primaryDs.setUsername("sa");
        primaryDs.setPassword("");

        org.springframework.jdbc.core.JdbcTemplate primaryJdbc = new org.springframework.jdbc.core.JdbcTemplate(primaryDs);
        primaryJdbc.execute("CREATE TABLE IF NOT EXISTS users (id VARCHAR(255) PRIMARY KEY, status VARCHAR(50))");
        primaryJdbc.execute("CREATE TABLE IF NOT EXISTS fractal_shard_topology (shard_name VARCHAR(100) PRIMARY KEY, status VARCHAR(50) NOT NULL, registered_at TIMESTAMP NOT NULL)");
        primaryJdbc.execute("MERGE INTO fractal_shard_topology KEY(shard_name) VALUES ('shard-1', 'ACTIVE', CURRENT_TIMESTAMP)");
        primaryJdbc.execute("MERGE INTO fractal_shard_topology KEY(shard_name) VALUES ('shard-2', 'ACTIVE', CURRENT_TIMESTAMP)");
        primaryJdbc.execute("MERGE INTO users KEY(id) VALUES ('tenant-free', 'ACTIVE')");
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws Exception {
        if (fractalRebalanceExecutor != null) {
            long deadline = System.currentTimeMillis() + 3000;
            while ((fractalRebalanceExecutor.getActiveCount() > 0 || fractalRebalanceExecutor.getThreadPoolExecutor().getQueue().size() > 0)
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
        }
        ShardContextHolder.clear();
        topologyManager.markTenantActive("tenant-locked");
        topologyManager.markTenantActive("tenant-restored");
        topologyManager.getMigrationStatusCache().invalidateAll();
    }

    @AfterEach
    void tearDown() {
        ShardContextHolder.clear();
        topologyManager.markTenantActive("tenant-locked");
        topologyManager.markTenantActive("tenant-restored");
        topologyManager.getMigrationStatusCache().invalidateAll();
    }

    @Test
    void shouldAllowAccessWhenTenantIsActive() {
        String shard = workService.processTenant("tenant-free");
        assertThat(shard).isNotBlank().startsWith("shard-");
    }

    @Test
    void shouldThrowTenantMigratingExceptionWhenTenantIsMigrating() {
        topologyManager.markTenantMigrating("tenant-locked");
        assertThatThrownBy(() -> workService.processTenant("tenant-locked"))
                .isInstanceOf(TenantMigratingException.class)
                .hasMessageContaining("Tenant 'tenant-locked' is currently undergoing shard rebalancing");
        assertThat(ShardContextHolder.getShard()).isNull();
    }

    @Test
    void shouldRestoreAccessWhenTenantFinishesMigration() {
        String tenant = "tenant-restored";
        topologyManager.markTenantMigrating(tenant);
        assertThatThrownBy(() -> workService.processTenant(tenant))
                .isInstanceOf(TenantMigratingException.class);

        // Mark active again
        topologyManager.markTenantActive(tenant);
        String shard = workService.processTenant(tenant);
        assertThat(shard).isNotBlank().startsWith("shard-");
        assertThat(ShardContextHolder.getShard()).isNull();
    }

    @Test
    void shouldTrackInFlightRequestsDuringExecutionAndDecrementAfterwards() {
        String tenant = "tenant-inflight-check";
        topologyManager.setRebalanceActive(true);
        try {
            long inFlightInside = workService.checkInFlightCount(tenant, topologyManager);
            assertThat(inFlightInside).isEqualTo(1L);
            assertThat(topologyManager.getInFlightRequestCount(tenant)).isEqualTo(0L);
        } finally {
            topologyManager.setRebalanceActive(false);
        }
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({ShardedWorkService.class, FractalAutoConfiguration.class})
    static class TestApp {}

    @Service
    static class ShardedWorkService {
        @Sharded(key = "#tenantId")
        public String processTenant(String tenantId) {
            return ShardContextHolder.getShard();
        }

        @Sharded(key = "#tenantId")
        public long checkInFlightCount(String tenantId, TopologyManager topologyManager) {
            return topologyManager.getInFlightRequestCount(tenantId);
        }
    }
}
