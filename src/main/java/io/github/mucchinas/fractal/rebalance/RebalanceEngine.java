package io.github.mucchinas.fractal.rebalance;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mucchinas.fractal.config.FractalProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.util.*;

public class RebalanceEngine {

    private final JdbcTemplate primaryJdbcTemplate;
    private final TableDependencyResolver dependencyResolver;
    private final TopologyManager topologyManager;
    private final FractalProperties.RebalancerProperties props;
    private final Map<String, NamedParameterJdbcTemplate> shardTemplates = new HashMap<>();

    public RebalanceEngine(DataSource primaryDataSource,
                           TableDependencyResolver dependencyResolver,
                           FractalProperties properties) {
        this(primaryDataSource, dependencyResolver, new TopologyManager(primaryDataSource), properties);
    }

    public RebalanceEngine(DataSource primaryDataSource,
                           TableDependencyResolver dependencyResolver,
                           TopologyManager topologyManager,
                           FractalProperties properties) {
        this(primaryDataSource, dependencyResolver, topologyManager, properties != null ? properties.getRebalancer() : new FractalProperties.RebalancerProperties());
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

    public RebalanceEngine(DataSource primaryDataSource,
                           TableDependencyResolver dependencyResolver,
                           TopologyManager topologyManager,
                           FractalProperties.RebalancerProperties props) {
        this.primaryJdbcTemplate = new JdbcTemplate(primaryDataSource);
        this.dependencyResolver = dependencyResolver;
        this.topologyManager = topologyManager;
        this.props = props != null ? props : new FractalProperties.RebalancerProperties();
    }

    public RebalanceEngine(DataSource primaryDataSource,
                           Map<String, DataSource> shardDataSources,
                           TableDependencyResolver dependencyResolver,
                           TopologyManager topologyManager,
                           FractalProperties.RebalancerProperties props) {
        this(primaryDataSource, dependencyResolver, topologyManager, props);
        if (shardDataSources != null) {
            shardDataSources.forEach((name, ds) ->
                    this.shardTemplates.put(name, new NamedParameterJdbcTemplate(ds))
            );
        }
    }

    public void executeMigration(List<MigrationDeltaCalculator.MigrationAction> actions) {
        if (actions == null || actions.isEmpty()) return;
        List<TableMigrationPlan> insertPlans = dependencyResolver.resolveMigrationPlans(
                props.getRootTable(),
                props.getRootIdColumn(),
                props.getShardedTables(),
                props.getReplicaTables(),
                props.getExcludeTables(),
                props.isShardAll()
        );
        List<TableMigrationPlan> deletePlans = new ArrayList<>(insertPlans);
        Collections.reverse(deletePlans);

        if (topologyManager != null) {
            topologyManager.setRebalanceActive(true);
            for (MigrationDeltaCalculator.MigrationAction action : actions) {
                topologyManager.recordPendingMigration(action.userId(), action.sourceShard(), action.targetShard());
            }
        }

        try {
            for (MigrationDeltaCalculator.MigrationAction action : actions) {
                String userId = action.userId();
                try {
                    System.out.println("FRACTAL: Migrazione utente " + userId +
                            " da " + action.sourceShard() + " a " + action.targetShard());
                    if (topologyManager != null) {
                        topologyManager.markTenantMigrating(userId);
                    }
                    setTenantStatus(userId, props.getMigratingValue());
                    if (topologyManager != null) {
                        java.time.Duration drainTimeout = props.getDrainTimeout() != null ? props.getDrainTimeout() : java.time.Duration.ofSeconds(10);
                        boolean drained = topologyManager.awaitTenantQuiescence(userId, drainTimeout);
                        if (!drained) {
                            System.err.println("FRACTAL: In-flight requests for tenant " + userId +
                                    " did not drain within " + drainTimeout + ". Deferring migration.");
                            setTenantStatus(userId, props.getActiveValue());
                            topologyManager.markTenantActive(userId);
                            continue;
                        }
                    }
                    if (props.getQuiescencePeriod() != null && !props.getQuiescencePeriod().isZero() && !props.getQuiescencePeriod().isNegative()) {
                        try {
                            Thread.sleep(props.getQuiescencePeriod().toMillis());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                    NamedParameterJdbcTemplate source = shardTemplates.get(action.sourceShard());
                    NamedParameterJdbcTemplate target = shardTemplates.get(action.targetShard());

                    if (source == null || target == null) {
                        System.err.println("FRACTAL: Shard non trovato: source=" + action.sourceShard() + ", target=" + action.targetShard());
                        continue;
                    }

                    String existingPhase = topologyManager != null ? topologyManager.getMigrationPhase(userId) : null;
                    boolean sourceHasData = hasRootRecord(userId, props.getRootTable(), props.getRootIdColumn(), source);
                    boolean targetHasData = hasRootRecord(userId, props.getRootTable(), props.getRootIdColumn(), target);

                    if (!sourceHasData && targetHasData) {
                        System.out.println("FRACTAL: Utente " + userId + " già presente su " + action.targetShard() + ", migrazione già completata.");
                    } else if (TopologyManager.PHASE_PRUNING.equalsIgnoreCase(existingPhase)) {
                        System.out.println("FRACTAL: Ripresa migrazione da fase PRUNING per " + userId);
                        for (TableMigrationPlan plan : deletePlans) {
                            deleteTableData(userId, plan, source);
                        }
                    } else {
                        if (topologyManager != null) {
                            topologyManager.recordMigrationStart(userId, action.sourceShard(), action.targetShard());
                        }
                        for (TableMigrationPlan plan : deletePlans) {
                            deleteTableData(userId, plan, target);
                        }
                        for (TableMigrationPlan plan : insertPlans) {
                            copyTableData(userId, plan, source, target);
                        }
                        if (topologyManager != null) {
                            topologyManager.updateMigrationPhase(userId, TopologyManager.PHASE_PRUNING);
                        }
                        for (TableMigrationPlan plan : deletePlans) {
                            deleteTableData(userId, plan, source);
                        }
                    }
                    if (topologyManager != null) {
                        topologyManager.completePendingMigration(userId);
                    }
                    setTenantStatus(userId, props.getActiveValue());
                    if (topologyManager != null) {
                        topologyManager.markTenantActive(userId);
                    }
                    System.out.println("FRACTAL: Migrazione completata per " + userId);

                } catch (Exception e) {
                    System.err.println("FRACTAL: Errore critico durante la migrazione di " + userId);
                    e.printStackTrace();
                }
            }
        } finally {
            if (topologyManager != null) {
                topologyManager.clearPendingMigrations();
            }
        }
    }

    private void setTenantStatus(String userId, String status) {
        if (props.getRootTable() != null && props.getRootIdColumn() != null && props.getStatusColumn() != null) {
            String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ?",
                    props.getRootTable(), props.getStatusColumn(), props.getRootIdColumn());
            primaryJdbcTemplate.update(sql, status, userId);
        }
    }

    private void copyTableData(String userId, TableMigrationPlan plan,
                               NamedParameterJdbcTemplate source, NamedParameterJdbcTemplate target) {

        String selectSql = plan.selectSql();
        List<Map<String, Object>> rows = source.queryForList(selectSql, Map.of("userId", userId));

        if (rows.isEmpty()) return;

        Map<String, Object> firstRow = rows.get(0);
        StringJoiner columns = new StringJoiner(", ");
        StringJoiner placeholders = new StringJoiner(", ");

        for (String colName : firstRow.keySet()) {
            columns.add(colName);
            placeholders.add(":" + colName);
        }

        String insertSql = String.format("INSERT INTO %s (%s) VALUES (%s)", plan.tableName(), columns, placeholders);
        int columnCount = firstRow.keySet().size();
        int batchSize = props.calculateBatchSize(columnCount);
        for (int i = 0; i < rows.size(); i += batchSize) {
            List<Map<String, Object>> chunk = rows.subList(i, Math.min(i + batchSize, rows.size()));
            MapSqlParameterSource[] batchArgs = chunk.stream()
                    .map(MapSqlParameterSource::new)
                    .toArray(MapSqlParameterSource[]::new);
            target.batchUpdate(insertSql, batchArgs);
        }
    }

    private void deleteTableData(String userId, TableMigrationPlan plan, NamedParameterJdbcTemplate source) {
        source.update(plan.deleteSql(), Map.of("userId", userId));
    }

    private boolean hasRootRecord(String userId, String rootTable, String rootIdColumn, NamedParameterJdbcTemplate template) {
        if (template == null || rootTable == null || rootIdColumn == null) {
            return false;
        }
        try {
            String sql = String.format("SELECT COUNT(*) FROM %s WHERE %s = :userId", rootTable, rootIdColumn);
            Integer count = template.queryForObject(sql, Map.of("userId", userId), Integer.class);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }
}