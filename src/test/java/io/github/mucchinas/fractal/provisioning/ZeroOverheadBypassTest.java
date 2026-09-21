package io.github.mucchinas.fractal.provisioning;

import io.github.mucchinas.fractal.annotation.Sharded;
import io.github.mucchinas.fractal.config.FractalAutoConfiguration;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = ZeroOverheadBypassTest.TestApp.class, properties = {
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:zero_overhead_primary",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",
        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:zero_overhead_shard1",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password="
})
class ZeroOverheadBypassTest {

    @Autowired
    private RegularBusinessService businessService;

    @SpyBean
    private TenantProvisioner tenantProvisioner;

    @AfterEach
    void tearDown() {
        ShardContextHolder.clear();
    }

    @Test
    void standardShardedMethodShouldBypassTenantProvisionerWithZeroOverhead() {
        String resultShard = businessService.findData("tenant-normal");
        assertThat(resultShard).isEqualTo("shard-1");

        // Verify TenantProvisioner.ensureProvisioned is NEVER called!
        verify(tenantProvisioner, never()).ensureProvisioned(any(), any());
        verify(tenantProvisioner, never()).provisionIfAbsent(any(), any());
    }

    @Service
    public static class RegularBusinessService {
        // provision defaults to false
        @Sharded(key = "#tenantId")
        public String findData(String tenantId) {
            return ShardContextHolder.getShard();
        }
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({FractalAutoConfiguration.class, RegularBusinessService.class})
    public static class TestApp {}
}
