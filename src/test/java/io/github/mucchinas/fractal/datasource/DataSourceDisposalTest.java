package io.github.mucchinas.fractal.datasource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.rebalance.TopologyManager;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DataSourceDisposalTest {

    @Test
    void shouldCloseAllUnderlyingHikariPoolsWhenDestroyed() {
        HikariConfig primaryConfig = new HikariConfig();
        primaryConfig.setPoolName("primary-test-pool");
        primaryConfig.setJdbcUrl("jdbc:h2:mem:disposal_primary;DB_CLOSE_DELAY=-1");
        primaryConfig.setUsername("sa");
        primaryConfig.setPassword("");
        HikariDataSource primaryDs = new HikariDataSource(primaryConfig);

        HikariConfig shard1Config = new HikariConfig();
        shard1Config.setPoolName("shard1-test-pool");
        shard1Config.setJdbcUrl("jdbc:h2:mem:disposal_shard1;DB_CLOSE_DELAY=-1");
        shard1Config.setUsername("sa");
        shard1Config.setPassword("");
        HikariDataSource shard1Ds = new HikariDataSource(shard1Config);

        ShardingRoutingDataSource routingDs = new ShardingRoutingDataSource();
        Map<Object, Object> targets = new HashMap<>();
        targets.put(TopologyManager.PRIMARY_SHARD_NAME, primaryDs);
        targets.put("shard-1", shard1Ds);

        routingDs.setPrimaryDataSource(primaryDs);
        routingDs.setShardDataSources(Map.of("shard-1", shard1Ds));
        routingDs.setDefaultTargetDataSource(primaryDs);
        routingDs.setTargetDataSources(targets);
        routingDs.afterPropertiesSet();

        assertThat(primaryDs.isClosed()).isFalse();
        assertThat(shard1Ds.isClosed()).isFalse();

        // Simulate Spring context shutdown
        routingDs.destroy();

        assertThat(primaryDs.isClosed()).isTrue();
        assertThat(shard1Ds.isClosed()).isTrue();
    }
}
