package io.github.mucchinas.fractal.rebalance;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mucchinas.fractal.config.FractalProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.*;

public class ReplicaTableSynchronizer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReplicaTableSynchronizer.class);

    private final NamedParameterJdbcTemplate primaryTemplate;
    private final Map<String, NamedParameterJdbcTemplate> shardTemplates = new HashMap<>();
    private final FractalProperties.RebalancerProperties rebalancerProps;
    private final Set<String> decommissionedShards;
    private final List<AutoCloseable> internalDataSources = new ArrayList<>();

    public ReplicaTableSynchronizer(DataSource primaryDataSource, FractalProperties properties) {
        this(primaryDataSource,
             extractShardDataSources(properties),
             properties != null ? properties.getRebalancer() : null,
             properties != null ? properties.getDecommissioningShardNames() : null);
    }

    private static Map<String, DataSource> extractShardDataSources(FractalProperties properties) {
        if (properties == null || properties.getShards() == null) {
            return Collections.emptyMap();
        }
        Map<String, DataSource> shards = new HashMap<>();
        properties.getShards().forEach((name, dbProps) -> {
            HikariConfig config = new HikariConfig();
            config.setPoolName("HikariPool-replica-" + name);
            config.setJdbcUrl(dbProps.getJdbcUrl());
            config.setUsername(dbProps.getUsername());
            config.setPassword(dbProps.getPassword());
            config.setConnectionTestQuery("SELECT 1");
            shards.put(name, new HikariDataSource(config));
        });
        return shards;
    }

    public ReplicaTableSynchronizer(DataSource primaryDataSource, Map<String, DataSource> shardDataSources) {
        this(primaryDataSource, shardDataSources, new FractalProperties.RebalancerProperties(), Collections.emptySet());
    }

    public ReplicaTableSynchronizer(DataSource primaryDataSource, Map<String, DataSource> shardDataSources, FractalProperties.RebalancerProperties rebalancerProps) {
        this(primaryDataSource, shardDataSources, rebalancerProps, Collections.emptySet());
    }

    public ReplicaTableSynchronizer(DataSource primaryDataSource,
                                    Map<String, DataSource> shardDataSources,
                                    FractalProperties.RebalancerProperties rebalancerProps,
                                    Set<String> decommissionedShards) {
        this.primaryTemplate = new NamedParameterJdbcTemplate(primaryDataSource);
        this.rebalancerProps = rebalancerProps != null ? rebalancerProps : new FractalProperties.RebalancerProperties();
        this.decommissionedShards = decommissionedShards != null ? Collections.unmodifiableSet(new HashSet<>(decommissionedShards)) : Collections.emptySet();
        if (shardDataSources != null) {
            shardDataSources.forEach((name, ds) -> {
                this.shardTemplates.put(name, new NamedParameterJdbcTemplate(ds));
                if (ds instanceof AutoCloseable closeable) {
                    this.internalDataSources.add(closeable);
                }
            });
        }
    }

    public void syncAllReplicaTables(List<String> replicaTables) {
        if (replicaTables == null || replicaTables.isEmpty() || shardTemplates.isEmpty()) {
            return;
        }

        for (String table : replicaTables) {
            if (table != null && !table.isBlank()) {
                syncTable(table.trim().toLowerCase());
            }
        }
    }

    public void syncAllReplicaTablesToShard(String shardName, List<String> replicaTables) {
        if (replicaTables == null || replicaTables.isEmpty() || shardName == null) {
            return;
        }

        NamedParameterJdbcTemplate target = shardTemplates.get(shardName);
        if (target == null) {
            log.error("FRACTAL: Target shard '{}' not found for replica table synchronization.", shardName);
            return;
        }

        for (String table : replicaTables) {
            if (table != null && !table.isBlank()) {
                syncTableToShard(table.trim().toLowerCase(), target);
            }
        }
    }

    public void syncTable(String tableName) {
        if (tableName == null || tableName.isBlank() || shardTemplates.isEmpty()) {
            return;
        }

        String normTable = tableName.trim().toLowerCase();
        List<Map<String, Object>> rows = fetchRowsFromPrimary(normTable);
        if (rows == null) {
            return;
        }

        for (Map.Entry<String, NamedParameterJdbcTemplate> entry : shardTemplates.entrySet()) {
            if (decommissionedShards.contains(entry.getKey())) {
                continue;
            }
            syncRowsToTarget(normTable, rows, entry.getValue());
        }
        log.info("FRACTAL: Replicated reference table '{}' ({} rows) synchronized to all active shards.", normTable, rows.size());
    }

    private void syncTableToShard(String tableName, NamedParameterJdbcTemplate target) {
        List<Map<String, Object>> rows = fetchRowsFromPrimary(tableName);
        if (rows != null) {
            syncRowsToTarget(tableName, rows, target);
        }
    }

    private List<Map<String, Object>> fetchRowsFromPrimary(String tableName) {
        try {
            String selectSql = "SELECT * FROM " + tableName;
            return primaryTemplate.queryForList(selectSql, Collections.emptyMap());
        } catch (Exception e) {
            log.warn("FRACTAL: Warning - unable to read replica table '{}' from primary database: {}", tableName, e.getMessage());
            return null;
        }
    }

    private void syncRowsToTarget(String tableName, List<Map<String, Object>> rows, NamedParameterJdbcTemplate target) {
        DataSource ds = target.getJdbcTemplate().getDataSource();
        if (ds == null) {
            log.error("FRACTAL: Cannot synchronize replica table '{}': DataSource is null.", tableName);
            return;
        }

        PlatformTransactionManager tm = new DataSourceTransactionManager(ds);
        TransactionTemplate tx = new TransactionTemplate(tm);
        try {
            tx.execute(status -> {
                target.getJdbcTemplate().execute("DELETE FROM " + tableName);

                if (rows.isEmpty()) {
                    return null;
                }

                Map<String, Object> firstRow = rows.get(0);
                StringJoiner columns = new StringJoiner(", ");
                StringJoiner placeholders = new StringJoiner(", ");

                for (String colName : firstRow.keySet()) {
                    columns.add(colName);
                    placeholders.add(":" + colName);
                }

                String insertSql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);

                int columnCount = firstRow.keySet().size();
                int batchSize = rebalancerProps.calculateBatchSize(columnCount);
                for (int i = 0; i < rows.size(); i += batchSize) {
                    List<Map<String, Object>> chunk = rows.subList(i, Math.min(i + batchSize, rows.size()));
                    MapSqlParameterSource[] batchArgs = chunk.stream()
                            .map(MapSqlParameterSource::new)
                            .toArray(MapSqlParameterSource[]::new);
                    target.batchUpdate(insertSql, batchArgs);
                }
                return null;
            });
        } catch (Exception e) {
            log.error("FRACTAL: Error synchronizing replica table '{}' to shard: {}", tableName, e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        for (AutoCloseable ac : internalDataSources) {
            try {
                ac.close();
            } catch (Exception ignored) {
            }
        }
        internalDataSources.clear();
    }
}
