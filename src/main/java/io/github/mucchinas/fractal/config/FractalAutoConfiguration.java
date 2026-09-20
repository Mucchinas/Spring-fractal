package io.github.mucchinas.fractal.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mucchinas.fractal.security.JwtSecurityKeyExtractor;
import io.github.mucchinas.fractal.aop.ShardingAspect;
import io.github.mucchinas.fractal.aop.ShardedBroadcastAspect;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import io.github.mucchinas.fractal.core.ShardingKeyExtractor;
import io.github.mucchinas.fractal.datasource.ShardingRoutingDataSource;
import org.springframework.beans.factory.ObjectProvider;
import io.github.mucchinas.fractal.rebalance.MigrationDeltaCalculator;
import io.github.mucchinas.fractal.rebalance.RebalanceEngine;
import io.github.mucchinas.fractal.rebalance.ReplicaTableSynchronizer;
import io.github.mucchinas.fractal.rebalance.TableDependencyResolver;
import io.github.mucchinas.fractal.rebalance.TopologyManager;
import io.github.mucchinas.fractal.rebalance.EntityMetadataResult;
import io.github.mucchinas.fractal.rebalance.EntityTableMetadataResolver;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@AutoConfiguration(before = DataSourceAutoConfiguration.class)
@EnableConfigurationProperties(FractalProperties.class)
@ConditionalOnProperty(prefix = "fractal.sharding", name = "enabled", havingValue = "true", matchIfMissing = true)
public class FractalAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ConsistentHashRouter consistentHashRouter(FractalProperties properties) {
        if (properties.getShards() == null || properties.getShards().isEmpty()) {
            throw new IllegalStateException("Fractal Sharding: No configured shard in application.yml!");
        }
        Set<String> activeShards = properties.getActiveShardNames();
        if (activeShards.isEmpty()) {
            throw new IllegalStateException("Fractal Sharding: At least one active shard must be configured (all configured shards are marked for decommissioning)!");
        }
        return new ConsistentHashRouter(activeShards, properties.getVirtualNodes());
    }

    @Bean
    @ConditionalOnMissingBean
    public ShardingAspect shardingAspect(ConsistentHashRouter router,
                                         ObjectProvider<ShardingKeyExtractor> extractors,
                                         ObjectProvider<TopologyManager> topologyManagerProvider,
                                         ObjectProvider<FractalProperties> propertiesProvider) {
        return new ShardingAspect(router, extractors, topologyManagerProvider, propertiesProvider);
    }

    @Bean
    @Primary
    public DataSource dataSource(FractalProperties properties) {
        ShardingRoutingDataSource routingDataSource = new ShardingRoutingDataSource();

        DataSource primaryDataSource = buildDataSource(properties.getPrimary());

        Map<Object, Object> targetDataSources = new HashMap<>();
        targetDataSources.put(TopologyManager.PRIMARY_SHARD_NAME, primaryDataSource);
        if (properties.getShards() != null) {
            properties.getShards().forEach((name, props) ->
                    targetDataSources.put(name, buildDataSource(props))
            );
        }

        routingDataSource.setDefaultTargetDataSource(primaryDataSource);
        routingDataSource.setTargetDataSources(targetDataSources);
        routingDataSource.afterPropertiesSet();

        return routingDataSource;
    }

    @Bean
    @ConditionalOnMissingBean
    public TopologyManager topologyManager(FractalProperties properties) {
        DataSource primary = buildDataSource(properties.getPrimary());
        boolean autoInitSchema = properties.getPrimary() == null || properties.getPrimary().isInitializeSchema();
        java.time.Duration cacheTtl = properties.getRebalancer() != null ? properties.getRebalancer().getStatusCacheTtl() : java.time.Duration.ofSeconds(2);
        long cacheMaxSize = properties.getRebalancer() != null ? properties.getRebalancer().getStatusCacheMaxSize() : 50_000L;
        return new TopologyManager(primary, autoInitSchema, cacheTtl, cacheMaxSize);
    }

    @Bean
    public ThreadPoolTaskExecutor fractalRebalanceExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(10);
        executor.setThreadNamePrefix("fractal-rebalancer-");
        executor.setThreadPriority(Thread.MIN_PRIORITY);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }

    @Bean
    @ConditionalOnMissingBean
    public EntityTableMetadataResolver entityTableMetadataResolver(ApplicationContext applicationContext) {
        return new EntityTableMetadataResolver(applicationContext);
    }

    @Bean
    @ConditionalOnMissingBean
    public TableDependencyResolver tableDependencyResolver(FractalProperties properties,
                                                           EntityTableMetadataResolver entityMetadataResolver) {
        DataSource primary = buildDataSource(properties.getPrimary());
        if (properties.getRebalancer().isShardAll()) {
            return new TableDependencyResolver(primary);
        }
        EntityMetadataResult entityResult = null;
        try {
            entityResult = entityMetadataResolver.resolve();
        } catch (Exception e) {
            System.err.println("FRACTAL: Warning during entity metadata resolution: " + e.getMessage());
        }
        if (entityResult != null && !entityResult.foreignKeys().isEmpty()) {
            return new TableDependencyResolver(primary, entityResult.foreignKeys());
        }
        return new TableDependencyResolver(primary);
    }

    @Bean
    @ConditionalOnMissingBean
    public RebalanceEngine rebalanceEngine(FractalProperties properties,
                                           TableDependencyResolver dependencyResolver,
                                           TopologyManager topologyManager) {
        DataSource primary = buildDataSource(properties.getPrimary());
        return new RebalanceEngine(primary, dependencyResolver, topologyManager, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public ReplicaTableSynchronizer replicaTableSynchronizer(FractalProperties properties) {
        DataSource primary = buildDataSource(properties.getPrimary());
        return new ReplicaTableSynchronizer(primary, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public ShardedBroadcastAspect shardedBroadcastAspect(FractalProperties properties) {
        return new ShardedBroadcastAspect(properties);
    }

    @Bean
    public CommandLineRunner fractalStartupListener(TopologyManager topologyManager,
                                                     TableDependencyResolver dependencyResolver,
                                                     RebalanceEngine rebalanceEngine,
                                                     ThreadPoolTaskExecutor fractalRebalanceExecutor,
                                                     FractalProperties properties,
                                                     EntityTableMetadataResolver entityMetadataResolver,
                                                     ReplicaTableSynchronizer replicaTableSynchronizer) {
        return args -> {
            if (properties.getRebalancer().isShardAll()) {
                List<String> replicaTables = dependencyResolver.discoverReplicaTables(
                        properties.getRebalancer().getRootTable(),
                        properties.getRebalancer().getReplicaTables(),
                        properties.getRebalancer().getExcludeTables(),
                        true
                );
                List<String> shardedTables = dependencyResolver.discoverShardedTables(
                        properties.getRebalancer().getRootTable(),
                        null,
                        replicaTables,
                        properties.getRebalancer().getExcludeTables(),
                        true
                );
                properties.getRebalancer().setReplicaTables(replicaTables);
                properties.getRebalancer().setShardedTables(shardedTables);
            } else {
                try {
                    EntityMetadataResult entityResult = entityMetadataResolver.resolve();
                    if (entityResult != null) {
                        if (entityResult.replicaTables() != null && !entityResult.replicaTables().isEmpty()) {
                            List<String> combinedReplicas = new ArrayList<>(properties.getRebalancer().getReplicaTables());
                            for (String rep : entityResult.replicaTables()) {
                                if (!combinedReplicas.contains(rep)) {
                                    combinedReplicas.add(rep);
                                }
                            }
                            properties.getRebalancer().setReplicaTables(combinedReplicas);
                        }
                        if (entityResult.rootTable() != null) {
                            if (properties.getRebalancer().getRootTable() == null) {
                                properties.getRebalancer().setRootTable(entityResult.rootTable());
                            }
                            if (properties.getRebalancer().getRootIdColumn() == null) {
                                properties.getRebalancer().setRootIdColumn(entityResult.rootIdColumn());
                            }
                            if (properties.getRebalancer().getStatusColumn() == null && entityResult.statusColumn() != null) {
                                properties.getRebalancer().setStatusColumn(entityResult.statusColumn());
                            }
                            if (entityResult.migratingValue() != null && !entityResult.migratingValue().isBlank()) {
                                properties.getRebalancer().setMigratingValue(entityResult.migratingValue());
                            }
                            if (entityResult.activeValue() != null && !entityResult.activeValue().isBlank()) {
                                properties.getRebalancer().setActiveValue(entityResult.activeValue());
                            }
                            if (properties.getRebalancer().getShardedTables() == null || properties.getRebalancer().getShardedTables().isEmpty()) {
                                properties.getRebalancer().setShardedTables(entityResult.shardedTables());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("FRACTAL: Warning during entity metadata resolution: " + e.getMessage());
                }
            }
            replicaTableSynchronizer.syncAllReplicaTables(properties.getRebalancer().getReplicaTables());

            if (properties.getRebalancer().isEnabled()) {
                topologyManager.initializeSchema();
                Set<String> allYamlShards = properties.getShards() != null ? properties.getShards().keySet() : Set.of();
                Set<String> activeYamlShards = properties.getActiveShardNames();
                Set<String> decommissionYamlShards = properties.getDecommissioningShardNames();
                List<String> dbShards = topologyManager.getKnownShardsFromDb();

                List<String> unconfiguredDbShards = dbShards.stream()
                        .filter(s -> !allYamlShards.contains(s))
                        .toList();
                if (!unconfiguredDbShards.isEmpty()) {
                    System.err.println("FRACTAL: Warning: Detected shards in DB topology (" + unconfiguredDbShards +
                            ") not present in application.yml. If you intended to decommission a shard, do not delete it from YAML immediately; " +
                            "mark it with 'decommission: true' so Fractal can safely drain data to surviving shards.");
                }

                boolean drainPrimary = properties.getPrimary() != null
                        && properties.getPrimary().isDrain()
                        && !topologyManager.isPrimaryDrained();

                boolean hasNewShards = activeYamlShards.stream().anyMatch(s -> !dbShards.contains(s));
                boolean hasDecommissioningShards = decommissionYamlShards.stream().anyMatch(dbShards::contains);
                List<TopologyManager.PendingMigration> pendingMigrations = topologyManager.getPendingMigrations();
                boolean hasPending = !pendingMigrations.isEmpty();

                if (drainPrimary || hasNewShards || hasDecommissioningShards || hasPending) {
                    if (hasPending) {
                        System.out.println("FRACTAL: Rilevate " + pendingMigrations.size() + " migrazioni interrotte da completare in modo idempotente.");
                    }
                    if (drainPrimary) {
                        System.out.println("FRACTAL: Rilevato primary.drain = true. Avvio evacuazione automatica dei dati dal database primario agli shard attivi!");
                    }
                    if (hasNewShards) {
                        System.out.println("FRACTAL: Rilevata discrepanza topologia. Nuovi shard aggiunti nello YAML!");
                    }
                    if (hasDecommissioningShards) {
                        System.out.println("FRACTAL: Rilevata decommissione shard. Avvio drenaggio automatico dei nodi in decommission!");
                    }

                    fractalRebalanceExecutor.execute(() -> {
                        if (topologyManager.tryAcquireRebalanceLock(
                                properties.getRebalancer().getLockTimeout(),
                                properties.getRebalancer().getLockRefreshInterval())) {
                            try {
                                if (properties.getRebalancer().getRootTable() == null || properties.getRebalancer().getRootIdColumn() == null) {
                                    System.out.println("FRACTAL: Rebalancer abilitato ma root-table o root-id-column non configurati. Registrazione topologia.");
                                    if (drainPrimary) {
                                        topologyManager.markPrimaryDrained();
                                    }
                                    decommissionYamlShards.forEach(topologyManager::removeShard);
                                    activeYamlShards.forEach(s -> {
                                        topologyManager.registerNewShard(s);
                                        replicaTableSynchronizer.syncAllReplicaTablesToShard(s, properties.getRebalancer().getReplicaTables());
                                    });
                                    return;
                                }

                                if (drainPrimary) {
                                    topologyManager.markPrimaryDraining();
                                }

                                for (String s : decommissionYamlShards) {
                                    if (dbShards.contains(s)) {
                                        topologyManager.markShardDraining(s);
                                    }
                                }

                                if (!pendingMigrations.isEmpty()) {
                                    List<MigrationDeltaCalculator.MigrationAction> recoveryActions = pendingMigrations.stream()
                                            .map(p -> new MigrationDeltaCalculator.MigrationAction(p.tenantId(), p.sourceShard(), p.targetShard()))
                                            .toList();
                                    rebalanceEngine.executeMigration(recoveryActions);
                                }

                                if (drainPrimary) {
                                    ConsistentHashRouter activeRouter = new ConsistentHashRouter(activeYamlShards, properties.getVirtualNodes());
                                    String sql = "SELECT " + properties.getRebalancer().getRootIdColumn() + " FROM " + properties.getRebalancer().getRootTable();
                                    JdbcTemplate primaryTemplate = new JdbcTemplate(buildDataSource(properties.getPrimary()));
                                    List<MigrationDeltaCalculator.MigrationAction> primaryDrainActions = new ArrayList<>();
                                    primaryTemplate.query(sql, rs -> {
                                        String tenantId = rs.getString(1);
                                        String targetShard = activeRouter.routeNode(tenantId);
                                        if (targetShard != null) {
                                            primaryDrainActions.add(new MigrationDeltaCalculator.MigrationAction(
                                                    tenantId, TopologyManager.PRIMARY_SHARD_NAME, targetShard));
                                        }
                                    });

                                    rebalanceEngine.executeMigration(primaryDrainActions);

                                    if (!topologyManager.hasPendingMigrationsForShard(TopologyManager.PRIMARY_SHARD_NAME)) {
                                        topologyManager.markPrimaryDrained();
                                        System.out.println("FRACTAL: Database primario drenato con successo su tutti gli shard attivi.");
                                    } else {
                                        System.err.println("FRACTAL: Il database primario presenta ancora migrazioni pendenti, mantenuto in stato DRAINING.");
                                    }

                                    activeYamlShards.forEach(s -> {
                                        topologyManager.registerNewShard(s);
                                        replicaTableSynchronizer.syncAllReplicaTablesToShard(s, properties.getRebalancer().getReplicaTables());
                                    });
                                }

                                if (hasNewShards || hasDecommissioningShards) {
                                    MigrationDeltaCalculator calculator = new MigrationDeltaCalculator(
                                            buildDataSource(properties.getPrimary()),
                                            properties.getRebalancer()
                                    );
                                    List<MigrationDeltaCalculator.MigrationAction> actions =
                                            calculator.calculateDelta(dbShards, activeYamlShards, properties.getVirtualNodes());
                                    rebalanceEngine.executeMigration(actions);

                                    activeYamlShards.forEach(s -> {
                                        topologyManager.registerNewShard(s);
                                        replicaTableSynchronizer.syncAllReplicaTablesToShard(s, properties.getRebalancer().getReplicaTables());
                                    });

                                    for (String s : decommissionYamlShards) {
                                        if (!topologyManager.hasPendingMigrationsForShard(s)) {
                                            topologyManager.removeShard(s);
                                            System.out.println("FRACTAL: Shard " + s + " drenato con successo e rimosso dalla topologia.");
                                        } else {
                                            System.err.println("FRACTAL: Shard " + s + " presenta ancora migrazioni pendenti, mantenuto in stato DRAINING.");
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                System.err.println("FRACTAL: Errore durante l'esecuzione del rebalancing automatico: " + e.getMessage());
                            } finally {
                                topologyManager.releaseRebalanceLock();
                            }
                        }
                    });
                }
            }
        };
    }

    private DataSource buildDataSource(FractalProperties.DataSourceProperties props) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(props.getJdbcUrl());
        config.setUsername(props.getUsername());
        config.setPassword(props.getPassword());

        config.setConnectionTestQuery("SELECT 1");
        config.setMaxLifetime(900000);
        config.setKeepaliveTime(0);

        return new HikariDataSource(config);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = {
            "org.springframework.security.core.context.SecurityContextHolder",
            "org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken"
    })
    public static class FractalSecurityAutoConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public ShardingKeyExtractor jwtSecurityKeyExtractor(FractalProperties properties) {
            return new JwtSecurityKeyExtractor(properties.getJwt().getClaimName());
        }
    }
}