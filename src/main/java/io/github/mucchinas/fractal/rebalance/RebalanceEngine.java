package io.github.mucchinas.fractal.rebalance;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.*;

public class RebalanceEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RebalanceEngine.class);

    private final JdbcTemplate primaryJdbcTemplate;
    private final TableDependencyResolver dependencyResolver;
    private final TopologyManager topologyManager;
    private final FractalProperties.RebalancerProperties props;
    private final Map<String, NamedParameterJdbcTemplate> shardTemplates = new HashMap<>();
    private final List<AutoCloseable> internalDataSources = new ArrayList<>();
    private volatile ConsistentHashRouter router;

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
                config.setPoolName("HikariPool-rebalance-" + name);
                config.setJdbcUrl(dbProps.getJdbcUrl());
                config.setUsername(dbProps.getUsername());
                config.setPassword(dbProps.getPassword());
                config.setConnectionTestQuery("SELECT 1");
                HikariDataSource ds = new HikariDataSource(config);
                this.internalDataSources.add(ds);
                this.shardTemplates.put(name, new NamedParameterJdbcTemplate(ds));
            });
        }
    }

    public RebalanceEngine(DataSource primaryDataSource,
                           TableDependencyResolver dependencyResolver,
                           TopologyManager topologyManager,
                           FractalProperties.RebalancerProperties props) {
        this.primaryJdbcTemplate = primaryDataSource != null ? new JdbcTemplate(primaryDataSource) : null;
        this.dependencyResolver = dependencyResolver;
        this.topologyManager = topologyManager;
        this.props = props != null ? props : new FractalProperties.RebalancerProperties();
        if (primaryDataSource != null) {
            this.shardTemplates.put(TopologyManager.PRIMARY_SHARD_NAME, new NamedParameterJdbcTemplate(primaryDataSource));
        }
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
                    log.info("FRACTAL: Migrating tenant {} from {} to {}", userId, action.sourceShard(), action.targetShard());
                    if (topologyManager != null) {
                        topologyManager.markTenantMigrating(userId);
                    }
                    setTenantStatus(userId, props.getMigratingValue());
                    if (topologyManager != null) {
                        Duration drainTimeout = props.getDrainTimeout() != null ? props.getDrainTimeout() : Duration.ofSeconds(10);
                        boolean drained = topologyManager.awaitTenantQuiescence(userId, drainTimeout);
                        if (!drained) {
                            log.warn("FRACTAL: In-flight requests for tenant {} did not drain within {}. Deferring migration.", userId, drainTimeout);
                            setTenantStatus(userId, props.getActiveValue());
                            topologyManager.markTenantActive(userId);
                            continue;
                        }
                    }

                    Duration quiescence = props.getQuiescencePeriod();
                    if (quiescence != null && !quiescence.isZero() && !quiescence.isNegative()) {
                        long sleepMillis = quiescence.toMillis();
                        if (props.getStatusCacheTtl() != null && props.getStatusCacheTtl().toMillis() > sleepMillis) {
                            sleepMillis = props.getStatusCacheTtl().toMillis();
                        }
                        try {
                            Thread.sleep(sleepMillis);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                    NamedParameterJdbcTemplate source = shardTemplates.get(action.sourceShard());
                    NamedParameterJdbcTemplate target = shardTemplates.get(action.targetShard());

                    if (source == null || target == null) {
                        log.error("FRACTAL: Shard not found: source={}, target={}", action.sourceShard(), action.targetShard());
                        setTenantStatus(userId, props.getActiveValue());
                        if (topologyManager != null) {
                            topologyManager.clearMigrationRecord(userId);
                            topologyManager.markTenantActive(userId);
                        }
                        continue;
                    }

                    String existingPhase = topologyManager != null ? topologyManager.getMigrationPhase(userId) : null;
                    boolean sourceHasData = hasRootRecord(userId, props.getRootTable(), props.getRootIdColumn(), source);
                    boolean targetHasData = hasRootRecord(userId, props.getRootTable(), props.getRootIdColumn(), target);

                    if (!sourceHasData && targetHasData) {
                        log.info("FRACTAL: Tenant {} already present on {}, migration already completed.", userId, action.targetShard());
                    } else if (TopologyManager.PHASE_PRUNING.equalsIgnoreCase(existingPhase)) {
                        log.info("FRACTAL: Resuming migration from PRUNING phase for tenant {}", userId);
                        for (TableMigrationPlan plan : deletePlans) {
                            if (action.sourceShard().equalsIgnoreCase(TopologyManager.PRIMARY_SHARD_NAME)
                                    && plan.tableName().equalsIgnoreCase(props.getRootTable())) {
                                continue;
                            }
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
                            if (action.sourceShard().equalsIgnoreCase(TopologyManager.PRIMARY_SHARD_NAME)
                                    && plan.tableName().equalsIgnoreCase(props.getRootTable())) {
                                continue;
                            }
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
                    log.info("FRACTAL: Migration completed for tenant {}", userId);

                } catch (Exception e) {
                    log.error("FRACTAL: Critical error during migration of tenant {}", userId, e);
                    try {
                        String currentPhase = topologyManager != null ? topologyManager.getMigrationPhase(userId) : null;
                        if (!TopologyManager.PHASE_PRUNING.equalsIgnoreCase(currentPhase)) {
                            NamedParameterJdbcTemplate target = shardTemplates.get(action.targetShard());
                            if (target != null) {
                                for (TableMigrationPlan plan : deletePlans) {
                                    try {
                                        deleteTableData(userId, plan, target);
                                    } catch (Exception ignored) {
                                    }
                                }
                            }
                            if (topologyManager != null) {
                                topologyManager.clearMigrationRecord(userId);
                            }
                        }
                        setTenantStatus(userId, props.getActiveValue());
                        if (topologyManager != null) {
                            topologyManager.markTenantActive(userId);
                        }
                    } catch (Exception rollbackEx) {
                        log.error("FRACTAL: Error during rollback for tenant {}", userId, rollbackEx);
                    }
                }
            }
        } finally {
            if (topologyManager != null) {
                topologyManager.clearPendingMigrations();
            }
        }
    }

    private void setTenantStatus(String userId, String status) {
        if (primaryJdbcTemplate != null && props.getRootTable() != null && props.getRootIdColumn() != null && props.getStatusColumn() != null) {
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

    public ConsistentHashRouter getRouter() {
        return router;
    }

    public void setRouter(ConsistentHashRouter router) {
        this.router = router;
    }

    /**
     * Evacuates a single newly provisioned tenant from primary database to its assigned target shard at runtime.
     */
    public void drainTenantFromPrimary(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) return;
        if (router == null) {
            throw new IllegalStateException("Router not initialized in RebalanceEngine");
        }
        String targetShard = router.routeNode(tenantId);
        if (targetShard == null) {
            throw new IllegalStateException("No active shard available for tenant " + tenantId);
        }
        drainTenantFromPrimary(tenantId, targetShard);
    }

    public void drainTenantFromPrimary(String tenantId, String targetShard) {
        if (tenantId == null || tenantId.isBlank() || targetShard == null) return;
        executeMigration(List.of(
                new MigrationDeltaCalculator.MigrationAction(tenantId, TopologyManager.PRIMARY_SHARD_NAME, targetShard)
        ));
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