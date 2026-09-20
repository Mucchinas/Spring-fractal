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

    @Test
    void shouldReturnNullWhenKeyIsNull() {
        ConsistentHashRouter router = new ConsistentHashRouter(Set.of("shard-1", "shard-2"), 150);
        assertThat(router.routeNode(null)).isNull();
    }

    @Test
    void shouldReturnNullWhenRingIsEmpty() {
        ConsistentHashRouter emptyRouter = new ConsistentHashRouter(Set.of(), 150);
        assertThat(emptyRouter.routeNode("tenant-1")).isNull();

        ConsistentHashRouter nullRouter = new ConsistentHashRouter(null, 150);
        assertThat(nullRouter.routeNode("tenant-1")).isNull();
    }

    @Test
    void shouldDistributeKeysAcrossAllShardsReasonably() {
        Set<String> shards = Set.of("shard-1", "shard-2", "shard-3");
        ConsistentHashRouter router = new ConsistentHashRouter(shards, 150);

        java.util.Map<String, Integer> counts = new java.util.HashMap<>();
        for (String s : shards) {
            counts.put(s, 0);
        }

        int totalKeys = 1500;
        for (int i = 0; i < totalKeys; i++) {
            String shard = router.routeNode("tenant-key-" + i);
            counts.put(shard, counts.get(shard) + 1);
        }

        for (String s : shards) {
            // Each shard in a 3-shard cluster should get between 20% and 50% of the traffic
            assertThat(counts.get(s))
                    .as("Shard %s key count", s)
                    .isGreaterThan(300)
                    .isLessThan(700);
        }
    }
}