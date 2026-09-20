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

    @Test
    void shouldRouteKeysToSurvivingShardsWhenShardIsRemoved() {
        Set<String> initialShards = Set.of("shard-1", "shard-2", "shard-3");
        ConsistentHashRouter initialRouter = new ConsistentHashRouter(initialShards, 150);

        Set<String> survivingShards = Set.of("shard-2", "shard-3");
        ConsistentHashRouter contractedRouter = new ConsistentHashRouter(survivingShards, 150);

        int reassignedFromShard1 = 0;
        int preservedOnSurviving = 0;

        for (int i = 0; i < 500; i++) {
            String key = "tenant-" + i;
            String initialTarget = initialRouter.routeNode(key);
            String contractedTarget = contractedRouter.routeNode(key);

            assertThat(contractedTarget).isIn(survivingShards);

            if ("shard-1".equals(initialTarget)) {
                reassignedFromShard1++;
                assertThat(contractedTarget).isIn("shard-2", "shard-3");
            } else {
                // In consistent hashing, removing shard-1 should not reassign keys between surviving nodes
                if (initialTarget.equals(contractedTarget)) {
                    preservedOnSurviving++;
                }
            }
        }

        assertThat(reassignedFromShard1).isGreaterThan(100);
        assertThat(preservedOnSurviving).isGreaterThan(200);
    }
}