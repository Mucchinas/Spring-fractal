package io.github.mucchinas.fractal.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mucchinas.fractal.security.JwtSecurityKeyExtractor;
import io.github.mucchinas.fractal.aop.ShardingAspect;
import io.github.mucchinas.fractal.aop.ShardedBroadcastAspect;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import io.github.mucchinas.fractal.core.ShardingKeyExtractor;
import io.github.mucchinas.fractal.datasource.ShardingRoutingDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import io.github.mucchinas.fractal.rebalance.MigrationDeltaCalculator;
import io.github.mucchinas.fractal.rebalance.RebalanceEngine;
import io.github.mucchinas.fractal.rebalance.ReplicaTableSynchronizer;
import io.github.mucchinas.fractal.rebalance.TableDependencyResolver;
import io.github.mucchinas.fractal.rebalance.TopologyManager;
import io.github.mucchinas.fractal.provisioning.DefaultTenantProvisioner;
import io.github.mucchinas.fractal.provisioning.RootEntityAttributeExtractor;
import io.github.mucchinas.fractal.provisioning.RootEntityCustomizer;
import io.github.mucchinas.fractal.provisioning.TenantInitializer;
import io.github.mucchinas.fractal.provisioning.TenantProvisioner;
import io.github.mucchinas.fractal.rebalance.EntityMetadataResult;
import io.github.mucchinas.fractal.rebalance.EntityTableMetadataResolver;
import io.github.mucchinas.fractal.repository.ShardedRepositoryStartupValidator;
import io.github.mucchinas.fractal.rebalance.PhysicalSchemaAuditValidator;
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

    private static final Logger log = LoggerFactory.getLogger(FractalAutoConfiguration.class);

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
                                         ObjectProvider<FractalProperties> propertiesProvider,
                                         ObjectProvider<TenantProvisioner> tenantProvisionerProvider) {
        return new ShardingAspect(router, extractors, topologyManagerProvider, propertiesProvider, tenantProvisionerProvider);
    }

    @Bean
    @Primary
    public DataSource dataSource(FractalProperties properties) {
        ShardingRoutingDataSource routingDataSource = new ShardingRoutingDataSource();

        DataSource primaryDataSource = buildDataSource(TopologyManager.PRIMARY_SHARD_NAME, properties.getPrimary());
        Map<String, DataSource> shardDataSources = new HashMap<>();
        Map<Object, Object> targetDataSources = new HashMap<>();

        targetDataSources.put(TopologyManager.PRIMARY_SHARD_NAME, primaryDataSource);
        if (properties.getShards() != null) {
            properties.getShards().forEach((name, props) -> {
                DataSource ds = buildDataSource(name, props);
                shardDataSources.put(name, ds);
                targetDataSources.put(name, ds);
            });
        }

        routingDataSource.setPrimaryDataSource(primaryDataSource);
        routingDataSource.setShardDataSources(shardDataSources);
        routingDataSource.setDefaultTargetDataSource(primaryDataSource);
        routingDataSource.setTargetDataSources(targetDataSources);
        routingDataSource.afterPropertiesSet();

        return routingDataSource;
    }

    @Bean
    @ConditionalOnMissingBean
    public TopologyManager topologyManager(FractalProperties properties, DataSource dataSource) {
        DataSource primary = resolvePrimaryDataSource(dataSource, properties);
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
                                                           EntityTableMetadataResolver entityMetadataResolver,
                                                           DataSource dataSource) {
        DataSource primary = resolvePrimaryDataSource(dataSource, properties);
        if (properties.getRebalancer().isShardAll()) {
            return new TableDependencyResolver(primary);
        }
        EntityMetadataResult entityResult = null;
        try {
            entityResult = entityMetadataResolver.resolve();
        } catch (Exception e) {
            log.warn("FRACTAL: Warning during entity metadata resolution: {}", e.getMessage());
        }
        if (entityResult != null && !entityResult.foreignKeys().isEmpty()) {
            return new TableDependencyResolver(primary, entityResult.foreignKeys());
        }
        return new TableDependencyResolver(primary);
    }

    @Bean
    @ConditionalOnMissingBean
    public RootEntityAttributeExtractor rootEntityAttributeExtractor(FractalProperties properties,
                                                                     EntityTableMetadataResolver entityTableMetadataResolver) {
        return new RootEntityAttributeExtractor(properties, entityTableMetadataResolver);
    }

    @Bean
    @ConditionalOnMissingBean
    public TenantProvisioner tenantProvisioner(DataSource dataSource,
                                               FractalProperties properties,
                                               ConsistentHashRouter consistentHashRouter,
                                               RootEntityAttributeExtractor attributeExtractor,
                                               ObjectProvider<TenantInitializer> tenantInitializers,
                                               ObjectProvider<RootEntityCustomizer> rootEntityCustomizers) {
        DataSource primary = resolvePrimaryDataSource(dataSource, properties);
        Map<String, DataSource> shards = resolveShardDataSources(dataSource, properties);
        return new DefaultTenantProvisioner(primary, shards, consistentHashRouter, properties, attributeExtractor, tenantInitializers, rootEntityCustomizers);
    }

    @Bean
    @ConditionalOnMissingBean
    public RebalanceEngine rebalanceEngine(FractalProperties properties,
                                           TableDependencyResolver dependencyResolver,
                                           TopologyManager topologyManager,
                                           DataSource dataSource) {
        DataSource primary = resolvePrimaryDataSource(dataSource, properties);
        Map<String, DataSource> shards = resolveShardDataSources(dataSource, properties);
        RebalanceEngine engine = new RebalanceEngine(primary, shards, dependencyResolver, topologyManager, properties.getRebalancer());
        Set<String> activeShards = properties.getActiveShardNames();
        if (!activeShards.isEmpty()) {
            engine.setRouter(new ConsistentHashRouter(activeShards, properties.getVirtualNodes()));
        }
        return engine;
    }

    @Bean
    @ConditionalOnMissingBean
    public ReplicaTableSynchronizer replicaTableSynchronizer(FractalProperties properties,
                                                             DataSource dataSource) {
        DataSource primary = resolvePrimaryDataSource(dataSource, properties);
        Map<String, DataSource> shards = resolveShardDataSources(dataSource, properties);
        return new ReplicaTableSynchronizer(primary, shards, properties.getRebalancer(), properties.getDecommissioningShardNames());
    }

    @Bean
    @ConditionalOnMissingBean
    public ShardedBroadcastAspect shardedBroadcastAspect(FractalProperties properties) {
        return new ShardedBroadcastAspect(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public PhysicalSchemaAuditValidator physicalSchemaAuditValidator(DataSource dataSource,
                                                                     FractalProperties properties,
                                                                     TableDependencyResolver dependencyResolver) {
        Map<String, DataSource> shards = resolveShardDataSources(dataSource, properties);
        return new PhysicalSchemaAuditValidator(shards, properties, dependencyResolver);
    }

    @Bean
    @ConditionalOnClass(name = "org.springframework.data.jpa.repository.JpaRepository")
    @ConditionalOnMissingBean
    public ShardedRepositoryStartupValidator shardedRepositoryStartupValidator(ApplicationContext applicationContext,
                                                                               FractalProperties properties,
                                                                               ObjectProvider<EntityTableMetadataResolver> metadataResolverProvider) {
        return new ShardedRepositoryStartupValidator(applicationContext, properties, metadataResolverProvider);
    }

    @Bean
    public CommandLineRunner fractalStartupListener(TopologyManager topologyManager,
                                                     TableDependencyResolver dependencyResolver,
                                                     RebalanceEngine rebalanceEngine,
                                                     ThreadPoolTaskExecutor fractalRebalanceExecutor,
                                                     FractalProperties properties,
                                                     EntityTableMetadataResolver entityMetadataResolver,
                                                     ReplicaTableSynchronizer replicaTableSynchronizer,
                                                     ObjectProvider<ShardedRepositoryStartupValidator> shardedRepositoryValidatorProvider,
                                                     ObjectProvider<PhysicalSchemaAuditValidator> physicalSchemaAuditValidatorProvider,
                                                     DataSource dataSource) {
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
                    log.warn("FRACTAL: Warning during entity metadata resolution: {}", e.getMessage());
                }
            }

            ShardedRepositoryStartupValidator repoValidator = shardedRepositoryValidatorProvider.getIfAvailable();
            if (repoValidator != null) {
                repoValidator.validate();
            }
            PhysicalSchemaAuditValidator schemaAuditValidator = physicalSchemaAuditValidatorProvider.getIfAvailable();
            if (schemaAuditValidator != null) {
                schemaAuditValidator.validate();
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
                    log.warn("FRACTAL: Warning: Detected shards in DB topology ({}) not present in application.yml. " +
                            "If you intended to decommission a shard, do not delete it from YAML immediately; " +
                            "mark it with 'decommission: true' so Fractal can safely drain data to surviving shards.",
                            unconfiguredDbShards);
                }

                boolean drainPrimary = properties.getPrimary() != null
                        && properties.getPrimary().isDrain()
                        && !topologyManager.isPrimaryDrained();

                boolean continuousDrainPrimary = properties.getPrimary() != null
                        && properties.getPrimary().isDrain()
                        && properties.getPrimary().isContinuousDrain()
                        && topologyManager.isPrimaryDrained();

                boolean hasNewShards = activeYamlShards.stream().anyMatch(s -> !dbShards.contains(s));
                boolean hasDecommissioningShards = decommissionYamlShards.stream().anyMatch(dbShards::contains);
                List<TopologyManager.PendingMigration> pendingMigrations = topologyManager.getPendingMigrations();
                boolean hasPending = !pendingMigrations.isEmpty();

                if (drainPrimary || continuousDrainPrimary || hasNewShards || hasDecommissioningShards || hasPending) {
                    if (hasPending) {
                        log.info("FRACTAL: Detected {} interrupted migrations to complete idempotently.", pendingMigrations.size());
                    }
                    if (drainPrimary) {
                        log.info("FRACTAL: Detected primary.drain = true. Initiating automatic data evacuation from primary database to active shards!");
                    }
                    if (continuousDrainPrimary) {
                        log.info("FRACTAL: Detected primary.continuous-drain = true. Checking continuous primary drain reconciliation across active shards...");
                    }
                    if (hasNewShards) {
                        log.info("FRACTAL: Topology discrepancy detected: new shards added to configuration!");
                    }
                    if (hasDecommissioningShards) {
                        log.info("FRACTAL: Shard decommissioning detected: initiating automatic draining of decommissioning nodes!");
                    }

                    fractalRebalanceExecutor.execute(() -> {
                        if (topologyManager.tryAcquireRebalanceLock(
                                properties.getRebalancer().getLockTimeout(),
                                properties.getRebalancer().getLockRefreshInterval())) {
                            try {
                                if (properties.getRebalancer().getRootTable() == null || properties.getRebalancer().getRootIdColumn() == null) {
                                    log.info("FRACTAL: Rebalancer enabled but root-table or root-id-column not configured. Registering topology.");
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
                                    rebalanceEngine.setRouter(activeRouter);
                                    DataSource primaryDs = resolvePrimaryDataSource(dataSource, properties);
                                    Map<String, DataSource> shardDataSources = resolveShardDataSources(dataSource, properties);
                                    MigrationDeltaCalculator calculator = new MigrationDeltaCalculator(primaryDs, properties.getRebalancer());
                                    List<MigrationDeltaCalculator.MigrationAction> primaryDrainActions = calculator.calculatePrimaryMisalignmentDelta(
                                            activeRouter,
                                            shardDataSources,
                                            FractalProperties.DrainCheckMode.FULL_STREAMING,
                                            properties.getPrimary().getDrainBatchSize()
                                    );

                                    rebalanceEngine.executeMigration(primaryDrainActions);

                                    if (!topologyManager.hasPendingMigrationsForShard(TopologyManager.PRIMARY_SHARD_NAME)) {
                                        topologyManager.markPrimaryDrained();
                                        log.info("FRACTAL: Primary database drained successfully across all active shards.");
                                    } else {
                                        log.warn("FRACTAL: Primary database still has pending migrations, remaining in DRAINING state.");
                                    }

                                    activeYamlShards.forEach(s -> {
                                        topologyManager.registerNewShard(s);
                                        replicaTableSynchronizer.syncAllReplicaTablesToShard(s, properties.getRebalancer().getReplicaTables());
                                    });
                                }

                                if (continuousDrainPrimary) {
                                    ConsistentHashRouter activeRouter = new ConsistentHashRouter(activeYamlShards, properties.getVirtualNodes());
                                    rebalanceEngine.setRouter(activeRouter);
                                    DataSource primaryDs = resolvePrimaryDataSource(dataSource, properties);
                                    Map<String, DataSource> shardDataSources = resolveShardDataSources(dataSource, properties);
                                    MigrationDeltaCalculator calculator = new MigrationDeltaCalculator(primaryDs, properties.getRebalancer());
                                    List<MigrationDeltaCalculator.MigrationAction> continuousDrainActions = calculator.calculatePrimaryMisalignmentDelta(
                                            activeRouter,
                                            shardDataSources,
                                            properties.getPrimary().getDrainCheckMode(),
                                            properties.getPrimary().getDrainBatchSize()
                                    );

                                    if (!continuousDrainActions.isEmpty()) {
                                        log.info("FRACTAL: Continuous primary drain reconciliation detected {} unaligned tenant(s) on primary. Evacuating to shards...",
                                                continuousDrainActions.size());
                                        rebalanceEngine.executeMigration(continuousDrainActions);
                                        log.info("FRACTAL: Continuous primary drain reconciliation completed successfully.");
                                    } else {
                                        log.info("FRACTAL: Continuous primary drain reconciliation verified: primary and shards are fully aligned.");
                                    }
                                }

                                if (hasNewShards || hasDecommissioningShards) {
                                    DataSource primaryDs = resolvePrimaryDataSource(dataSource, properties);
                                    MigrationDeltaCalculator calculator = new MigrationDeltaCalculator(
                                            primaryDs,
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
                                            log.info("FRACTAL: Shard '{}' drained successfully and removed from topology.", s);
                                        } else {
                                            log.warn("FRACTAL: Shard '{}' still has pending migrations, remaining in DRAINING state.", s);
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                log.error("FRACTAL: Error executing automatic rebalancing: {}", e.getMessage(), e);
                            } finally {
                                topologyManager.releaseRebalanceLock();
                            }
                        }
                    });
                }
            }
        };
    }

    private DataSource resolvePrimaryDataSource(DataSource dataSource, FractalProperties properties) {
        if (dataSource instanceof ShardingRoutingDataSource srds && srds.getPrimaryDataSource() != null) {
            return srds.getPrimaryDataSource();
        }
        return buildDataSource(TopologyManager.PRIMARY_SHARD_NAME, properties.getPrimary());
    }

    private Map<String, DataSource> resolveShardDataSources(DataSource dataSource, FractalProperties properties) {
        if (dataSource instanceof ShardingRoutingDataSource srds && srds.getShardDataSources() != null && !srds.getShardDataSources().isEmpty()) {
            return srds.getShardDataSources();
        }
        Map<String, DataSource> shards = new HashMap<>();
        if (properties.getShards() != null) {
            properties.getShards().forEach((name, props) ->
                    shards.put(name, buildDataSource(name, props))
            );
        }
        return shards;
    }

    private DataSource buildDataSource(String name, FractalProperties.DataSourceProperties props) {
        if (props == null || props.getJdbcUrl() == null || props.getJdbcUrl().isBlank()) {
            throw new IllegalArgumentException("Fractal Sharding: DataSource configuration for '" + name + "' is missing or has no jdbcUrl defined!");
        }
        HikariConfig config = new HikariConfig();
        config.setPoolName("HikariPool-" + name);
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