package neko.mukynas.fractal.rebalance;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import neko.mukynas.fractal.config.FractalProperties;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.util.*;

/**
 * Synchronizes Replicated (Broadcast) Reference Tables from the primary coordinator database
 * to all configured physical shards.
 *
 * <p>Reference tables (such as currencies, countries, system roles, and categories) are not
 * partitioned by tenant. Instead, 100% of their rows are copied to every physical shard database,
 * allowing local relational SQL queries and JOINs on any shard without cross-database network calls.</p>
 */
public class ReplicaTableSynchronizer {

    private final NamedParameterJdbcTemplate primaryTemplate;
    private final Map<String, NamedParameterJdbcTemplate> shardTemplates = new HashMap<>();
    private final FractalProperties.RebalancerProperties rebalancerProps;

    public ReplicaTableSynchronizer(DataSource primaryDataSource, FractalProperties properties) {
        this.primaryTemplate = new NamedParameterJdbcTemplate(primaryDataSource);
        this.rebalancerProps = properties != null && properties.getRebalancer() != null
                ? properties.getRebalancer()
                : new FractalProperties.RebalancerProperties();

        if (properties != null && properties.getShards() != null) {
            properties.getShards().forEach((name, dbProps) -> {
                HikariConfig config = new HikariConfig();
                config.setJdbcUrl(dbProps.getJdbcUrl());
                config.setUsername(dbProps.getUsername());
                config.setPassword(dbProps.getPassword());
                this.shardTemplates.put(name, new NamedParameterJdbcTemplate(new HikariDataSource(config)));
            });
        }
    }

    public ReplicaTableSynchronizer(DataSource primaryDataSource, Map<String, DataSource> shardDataSources) {
        this(primaryDataSource, shardDataSources, new FractalProperties.RebalancerProperties());
    }

    public ReplicaTableSynchronizer(DataSource primaryDataSource, Map<String, DataSource> shardDataSources, FractalProperties.RebalancerProperties rebalancerProps) {
        this.primaryTemplate = new NamedParameterJdbcTemplate(primaryDataSource);
        this.rebalancerProps = rebalancerProps != null ? rebalancerProps : new FractalProperties.RebalancerProperties();
        if (shardDataSources != null) {
            shardDataSources.forEach((name, ds) ->
                    this.shardTemplates.put(name, new NamedParameterJdbcTemplate(ds))
            );
        }
    }

    /**
     * Synchronizes all designated replica tables from the primary database to all active shards.
     */
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

    /**
     * Synchronizes all designated replica tables from the primary database to a specific target shard.
     * Useful during cluster expansion when a newly added shard needs to be provisioned with reference data.
     */
    public void syncAllReplicaTablesToShard(String shardName, List<String> replicaTables) {
        if (replicaTables == null || replicaTables.isEmpty() || shardName == null) {
            return;
        }

        NamedParameterJdbcTemplate target = shardTemplates.get(shardName);
        if (target == null) {
            System.err.println("FRACTAL: Target shard '" + shardName + "' not found for replica table synchronization.");
            return;
        }

        for (String table : replicaTables) {
            if (table != null && !table.isBlank()) {
                syncTableToShard(table.trim().toLowerCase(), target);
            }
        }
    }

    /**
     * Synchronizes a single replica table from the primary database to all active shards.
     */
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
            syncRowsToTarget(normTable, rows, entry.getValue());
        }
        System.out.println("FRACTAL: Replicated reference table '" + normTable + "' (" + rows.size() + " rows) synchronized to all shards.");
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
            System.err.println("FRACTAL: Warning - unable to read replica table '" + tableName + "' from primary database: " + e.getMessage());
            return null;
        }
    }

    private void syncRowsToTarget(String tableName, List<Map<String, Object>> rows, NamedParameterJdbcTemplate target) {
        try {
            // Remove existing rows to ensure an exact replica of primary data without PK conflicts
            target.getJdbcTemplate().execute("DELETE FROM " + tableName);

            if (rows.isEmpty()) {
                return;
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
        } catch (Exception e) {
            System.err.println("FRACTAL: Error synchronizing replica table '" + tableName + "' to shard: " + e.getMessage());
        }
    }
}
