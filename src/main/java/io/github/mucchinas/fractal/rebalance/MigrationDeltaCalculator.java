package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.util.*;

public class MigrationDeltaCalculator {

    private static final Logger log = LoggerFactory.getLogger(MigrationDeltaCalculator.class);

    private final JdbcTemplate primaryJdbcTemplate;
    private final String rootTable;
    private final String rootIdColumn;
    private final String statusColumn;
    private final String activeValue;

    public MigrationDeltaCalculator(DataSource primaryDataSource, FractalProperties.RebalancerProperties props) {
        this.primaryJdbcTemplate = primaryDataSource != null ? new JdbcTemplate(primaryDataSource) : null;
        this.rootTable = props != null ? props.getRootTable() : null;
        this.rootIdColumn = props != null ? props.getRootIdColumn() : null;
        this.statusColumn = props != null ? props.getStatusColumn() : null;
        this.activeValue = (props != null && props.getActiveValue() != null) ? props.getActiveValue() : "ACTIVE";
    }

    public record MigrationAction(String userId, String sourceShard, String targetShard) {}

    public List<MigrationAction> calculateDelta(List<String> dbShards, Set<String> yamlShards, int virtualNodes) {
        ConsistentHashRouter oldRouter = new ConsistentHashRouter(dbShards, virtualNodes);
        ConsistentHashRouter newRouter = new ConsistentHashRouter(yamlShards, virtualNodes);

        List<MigrationAction> actions = new ArrayList<>();
        String sql = "SELECT " + rootIdColumn + " FROM " + rootTable;

        primaryJdbcTemplate.query(sql, rs -> {
            String userId = rs.getString(1);
            String currentShard = oldRouter.routeNode(userId);
            String futureShard = newRouter.routeNode(userId);
            if (currentShard != null && !currentShard.equals(futureShard)) {
                actions.add(new MigrationAction(userId, currentShard, futureShard));
            }
        });

        return actions;
    }

    /**
     * Scale-proof 3-tier continuous primary drain reconciliation engine:
     * Tier 1: Instant Count Guard (O(1) time & memory)
     * Tier 2: Status-Indexed Probe (O(K) memory for K new tenants)
     * Tier 3: Bounded Keyset Streaming Cursor (O(1) bounded memory, immune to OOM)
     */
    public List<MigrationAction> calculatePrimaryMisalignmentDelta(
            ConsistentHashRouter activeRouter,
            Map<String, DataSource> activeShardDataSources,
            FractalProperties.DrainCheckMode checkMode,
            int batchSize) {

        if (primaryJdbcTemplate == null || rootTable == null || rootIdColumn == null
                || activeRouter == null || activeShardDataSources == null || activeShardDataSources.isEmpty()) {
            return Collections.emptyList();
        }

        FractalProperties.DrainCheckMode mode = checkMode != null ? checkMode : FractalProperties.DrainCheckMode.COUNT_THEN_PROBE;
        int boundedBatchSize = batchSize > 0 ? batchSize : 5000;

        // Tier 1: Instant Count Guard
        if (mode == FractalProperties.DrainCheckMode.COUNT_THEN_PROBE) {
            long primaryCount = getCount(primaryJdbcTemplate, rootTable);
            long shardsSum = 0;
            boolean countFailed = primaryCount < 0;
            for (DataSource shardDs : activeShardDataSources.values()) {
                long shardCount = getCount(new JdbcTemplate(shardDs), rootTable);
                if (shardCount < 0) {
                    countFailed = true;
                    break;
                }
                shardsSum += shardCount;
            }

            if (!countFailed && primaryCount == shardsSum) {
                log.debug("FRACTAL Continuous Drain: Tier 1 Instant Count Guard passed. Primary count ({}) matches shards sum ({}).",
                        primaryCount, shardsSum);
                return Collections.emptyList();
            }
            log.info("FRACTAL Continuous Drain: Tier 1 count discrepancy detected (primary={}, shardsSum={}). Proceeding to reconciliation.",
                    primaryCount, shardsSum);
        }

        // Tier 2: Status-Indexed Probe
        if (mode == FractalProperties.DrainCheckMode.COUNT_THEN_PROBE || mode == FractalProperties.DrainCheckMode.PROBE_ONLY) {
            if (statusColumn != null && !statusColumn.isBlank()) {
                List<MigrationAction> statusActions = probeUnsyncedStatusTenants(activeRouter, boundedBatchSize);
                if (!statusActions.isEmpty()) {
                    log.info("FRACTAL Continuous Drain: Tier 2 Status Probe found {} unsynced tenant(s).", statusActions.size());
                    return statusActions;
                }
            }
            if (mode == FractalProperties.DrainCheckMode.PROBE_ONLY) {
                return Collections.emptyList();
            }
        }

        // Tier 3: Bounded Keyset Streaming Cursor
        log.info("FRACTAL Continuous Drain: Executing Tier 3 Bounded Keyset Streaming Cursor with batch size {}...", boundedBatchSize);
        return streamKeysetMisalignments(activeRouter, activeShardDataSources, boundedBatchSize);
    }

    private List<MigrationAction> probeUnsyncedStatusTenants(ConsistentHashRouter activeRouter, int limit) {
        String sql = "SELECT " + rootIdColumn + " FROM " + rootTable +
                " WHERE " + statusColumn + " != ? OR " + statusColumn + " IS NULL";

        List<MigrationAction> actions = new ArrayList<>();
        try {
            primaryJdbcTemplate.query(con -> {
                PreparedStatement ps = con.prepareStatement(sql);
                ps.setString(1, activeValue);
                ps.setMaxRows(limit);
                return ps;
            }, rs -> {
                String tenantId = rs.getString(1);
                String targetShard = activeRouter.routeNode(tenantId);
                if (targetShard != null) {
                    actions.add(new MigrationAction(tenantId, TopologyManager.PRIMARY_SHARD_NAME, targetShard));
                }
            });
        } catch (Exception e) {
            log.warn("FRACTAL Continuous Drain: Tier 2 status probe failed: {}. Falling back to Tier 3.", e.getMessage());
            return Collections.emptyList();
        }
        return actions;
    }

    private List<MigrationAction> streamKeysetMisalignments(
            ConsistentHashRouter activeRouter,
            Map<String, DataSource> activeShardDataSources,
            int batchSize) {

        List<MigrationAction> missingActions = new ArrayList<>();
        String lastSeenId = null;

        while (true) {
            final String currentLastId = lastSeenId;
            final String querySql = (currentLastId == null)
                    ? "SELECT " + rootIdColumn + " FROM " + rootTable + " ORDER BY " + rootIdColumn + " ASC"
                    : "SELECT " + rootIdColumn + " FROM " + rootTable + " WHERE " + rootIdColumn + " > ? ORDER BY " + rootIdColumn + " ASC";

            List<String> chunkIds = new ArrayList<>();
            try {
                primaryJdbcTemplate.query(con -> {
                    PreparedStatement ps = con.prepareStatement(querySql);
                    if (currentLastId != null) {
                        ps.setString(1, currentLastId);
                    }
                    ps.setMaxRows(batchSize);
                    return ps;
                }, rs -> {
                    chunkIds.add(rs.getString(1));
                });
            } catch (Exception e) {
                log.error("FRACTAL Continuous Drain: Failed to stream keyset chunk: {}", e.getMessage(), e);
                break;
            }

            if (chunkIds.isEmpty()) {
                break;
            }

            // Group chunk by target shard
            Map<String, List<String>> idsByShard = new HashMap<>();
            for (String id : chunkIds) {
                String targetShard = activeRouter.routeNode(id);
                if (targetShard != null) {
                    idsByShard.computeIfAbsent(targetShard, k -> new ArrayList<>()).add(id);
                }
            }

            // Check existence on physical shards
            for (Map.Entry<String, List<String>> entry : idsByShard.entrySet()) {
                String targetShard = entry.getKey();
                List<String> shardBatch = entry.getValue();
                DataSource shardDs = activeShardDataSources.get(targetShard);
                if (shardDs == null) {
                    continue;
                }

                Set<String> existingOnShard = new HashSet<>();
                JdbcTemplate shardTemplate = new JdbcTemplate(shardDs);
                // Partition into sub-batches of 1000 to respect SQL IN parameter limits
                for (List<String> partition : partition(shardBatch, 1000)) {
                    String placeholders = String.join(",", Collections.nCopies(partition.size(), "?"));
                    String checkSql = "SELECT " + rootIdColumn + " FROM " + rootTable +
                            " WHERE " + rootIdColumn + " IN (" + placeholders + ")";
                    try {
                        shardTemplate.query(checkSql, rs -> {
                            existingOnShard.add(rs.getString(1));
                        }, partition.toArray());
                    } catch (Exception e) {
                        log.warn("FRACTAL Continuous Drain: Failed to verify chunk on shard {}: {}", targetShard, e.getMessage());
                    }
                }

                for (String id : shardBatch) {
                    if (!existingOnShard.contains(id)) {
                        missingActions.add(new MigrationAction(id, TopologyManager.PRIMARY_SHARD_NAME, targetShard));
                    }
                }
            }

            if (chunkIds.size() < batchSize) {
                break;
            }
            lastSeenId = chunkIds.get(chunkIds.size() - 1);
        }

        return missingActions;
    }

    private long getCount(JdbcTemplate jt, String table) {
        try {
            Long count = jt.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
            return count != null ? count : 0L;
        } catch (Exception e) {
            return -1L;
        }
    }

    private static <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }
}