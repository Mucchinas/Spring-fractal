package io.github.mucchinas.fractal.core;

import org.junit.jupiter.api.Test;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;

class ConsistentHashRouterTest {

    @Test
    void shouldRouteToSameShardConsistently() {
        Set<String> shards = Set.of("shard-1", "shard-2", "shard-3");
        ConsistentHashRouter router = new ConsistentHashRouter(shards, 150);

        String tenantId = "tenant-12345";
        String target1 = router.routeNode(tenantId);
        String target2 = router.routeNode(tenantId);
        assertThat(target1)
                .isNotNull()
                .isIn(shards)
                .isEqualTo(target2);
    }
}