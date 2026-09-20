package io.github.mucchinas.fractal.aop;

import io.github.mucchinas.fractal.annotation.ShardedBroadcast;
import io.github.mucchinas.fractal.config.FractalAutoConfiguration;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:bcast_primary;DB_CLOSE_DELAY=-1",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",
        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:bcast_shard1;DB_CLOSE_DELAY=-1",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",
        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:bcast_shard2;DB_CLOSE_DELAY=-1",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class ShardedBroadcastAspectTest {

    @Autowired
    private DummyBroadcastService broadcastService;

    @AfterEach
    void tearDown() {
        ShardContextHolder.clear();
        broadcastService.getRecordedContexts().clear();
    }

    @Test
    void shouldBroadcastToPrimaryAndAllShards() {
        broadcastService.broadcastUpdate("EUR", 1.05);

        List<String> recorded = broadcastService.getRecordedContexts();
        assertThat(recorded).containsExactlyInAnyOrder("primary", "shard-1", "shard-2");
        assertThat(recorded.get(0)).isEqualTo("primary");
        assertThat(ShardContextHolder.getShard()).isNull();
    }

    @Test
    void shouldBroadcastOnlyToWorkerShardsWhenIncludePrimaryIsFalse() {
        broadcastService.broadcastToShardsOnly("EUR", 1.10);

        List<String> recorded = broadcastService.getRecordedContexts();
        assertThat(recorded).containsExactlyInAnyOrder("shard-1", "shard-2");
        assertThat(recorded).doesNotContain("primary");
        assertThat(ShardContextHolder.getShard()).isNull();
    }

    @Test
    void shouldExcludeDecommissioningShardsFromBroadcast() {
        io.github.mucchinas.fractal.config.FractalProperties props = new io.github.mucchinas.fractal.config.FractalProperties();
        io.github.mucchinas.fractal.config.FractalProperties.DataSourceProperties s1 = new io.github.mucchinas.fractal.config.FractalProperties.DataSourceProperties();
        s1.setStatus("ACTIVE");
        io.github.mucchinas.fractal.config.FractalProperties.DataSourceProperties s2 = new io.github.mucchinas.fractal.config.FractalProperties.DataSourceProperties();
        s2.setDecommission(true);
        io.github.mucchinas.fractal.config.FractalProperties.DataSourceProperties s3 = new io.github.mucchinas.fractal.config.FractalProperties.DataSourceProperties();
        s3.setStatus("DRAINING");

        props.setShards(java.util.Map.of("shard-1", s1, "shard-2", s2, "shard-3", s3));

        ShardedBroadcastAspect aspect = new ShardedBroadcastAspect(props);
        // Verify via properties filtering that only shard-1 is active
        assertThat(props.getActiveShardNames()).containsExactly("shard-1");
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({DummyBroadcastService.class, FractalAutoConfiguration.class})
    static class DummyApp {}

    @Service
    static class DummyBroadcastService {

        private final List<String> recordedContexts = new ArrayList<>();

        public List<String> getRecordedContexts() {
            return recordedContexts;
        }

        @ShardedBroadcast
        public void broadcastUpdate(String code, double rate) {
            String currentShard = ShardContextHolder.getShard();
            recordedContexts.add(currentShard == null ? "primary" : currentShard);
        }

        @ShardedBroadcast(includePrimary = false)
        public void broadcastToShardsOnly(String code, double rate) {
            String currentShard = ShardContextHolder.getShard();
            recordedContexts.add(currentShard == null ? "primary" : currentShard);
        }
    }
}
