package neko.mukynas.fractal.aop;

import neko.mukynas.fractal.annotation.Sharded;
import neko.mukynas.fractal.config.FractalAutoConfiguration;
import neko.mukynas.fractal.core.ShardContextHolder;
import neko.mukynas.fractal.exception.TenantMigratingException;
import neko.mukynas.fractal.rebalance.TopologyManager;
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

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        org.springframework.jdbc.datasource.DriverManagerDataSource primaryDs = new org.springframework.jdbc.datasource.DriverManagerDataSource();
        primaryDs.setDriverClassName("org.h2.Driver");
        primaryDs.setUrl("jdbc:h2:mem:miglock_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        primaryDs.setUsername("sa");
        primaryDs.setPassword("");

        org.springframework.jdbc.core.JdbcTemplate primaryJdbc = new org.springframework.jdbc.core.JdbcTemplate(primaryDs);
        primaryJdbc.execute("CREATE TABLE IF NOT EXISTS users (id VARCHAR(255) PRIMARY KEY, status VARCHAR(50))");
        primaryJdbc.execute("MERGE INTO users KEY(id) VALUES ('tenant-free', 'ACTIVE')");
    }

    @AfterEach
    void tearDown() {
        ShardContextHolder.clear();
        topologyManager.markTenantActive("tenant-locked");
    }

    @Test
    void shouldAllowAccessWhenTenantIsActive() {
        String shard = workService.processTenant("tenant-free");
        assertThat(shard).isNotBlank().startsWith("shard-");
    }

    @Test
    void shouldThrowTenantMigratingExceptionWhenTenantIsMigrating() {
        // Mark tenant as migrating
        topologyManager.markTenantMigrating("tenant-locked");

        // Act & Assert: Aspect must block access to prevent dirty writes during migration
        assertThatThrownBy(() -> workService.processTenant("tenant-locked"))
                .isInstanceOf(TenantMigratingException.class)
                .hasMessageContaining("Tenant 'tenant-locked' is currently undergoing shard rebalancing");

        // ThreadLocal must remain clean
        assertThat(ShardContextHolder.getShard()).isNull();
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
    }
}
