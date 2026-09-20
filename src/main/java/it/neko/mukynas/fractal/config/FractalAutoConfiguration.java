package it.neko.mukynas.fractal.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import it.neko.mukynas.fractal.security.JwtSecurityKeyExtractor;
import it.neko.mukynas.fractal.aop.ShardingAspect;
import it.neko.mukynas.fractal.aop.ShardedBroadcastAspect;
import it.neko.mukynas.fractal.core.ConsistentHashRouter;
import it.neko.mukynas.fractal.core.ShardingKeyExtractor;
import it.neko.mukynas.fractal.datasource.ShardingRoutingDataSource;
import org.springframework.beans.factory.ObjectProvider;
import it.neko.mukynas.fractal.rebalance.MigrationDeltaCalculator;
import it.neko.mukynas.fractal.rebalance.RebalanceEngine;
import it.neko.mukynas.fractal.rebalance.ReplicaTableSynchronizer;
import it.neko.mukynas.fractal.rebalance.TableDependencyResolver;
import it.neko.mukynas.fractal.rebalance.TopologyManager;
import it.neko.mukynas.fractal.rebalance.EntityMetadataResult;
import it.neko.mukynas.fractal.rebalance.EntityTableMetadataResolver;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
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
        return new ConsistentHashRouter(properties.getShards().keySet(), properties.getVirtualNodes());
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
        // Costruiamo il datasource primario appositamente per le logiche di admin
        DataSource primary = buildDataSource(properties.getPrimary());
        boolean autoInitSchema = properties.getPrimary() == null || properties.getPrimary().isInitializeSchema();
        java.time.Duration cacheTtl = properties.getRebalancer() != null ? properties.getRebalancer().getStatusCacheTtl() : java.time.Duration.ofSeconds(2);
        long cacheMaxSize = properties.getRebalancer() != null ? properties.getRebalancer().getStatusCacheMaxSize() : 50_000L;
        return new TopologyManager(primary, autoInitSchema, cacheTtl, cacheMaxSize);
    }

    /**
     * Thread Pool dedicato ESCLUSIVAMENTE al Rebalancer.
     * È limitato a 1 singolo thread, con priorità minima per non rubare CPU alle richieste HTTP.
     */
    @Bean
    public ThreadPoolTaskExecutor fractalRebalanceExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(10); // Code piccolissime
        executor.setThreadNamePrefix("fractal-rebalancer-");
        // Abbassiamo la priorità del thread a livello sistema operativo/JVM
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
                // Infer root-table, root-id-column, replica-tables, and sharded-tables from entities if not explicitly configured in YAML
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

            // Synchronize all replica reference tables across physical shards on startup
            replicaTableSynchronizer.syncAllReplicaTables(properties.getRebalancer().getReplicaTables());

            if (properties.getRebalancer().isEnabled()) {
                // 1. Crea le tabelle se non esistono
                topologyManager.initializeSchema();

                // 2. Legge dal file properties gli shard attuali
                Set<String> yamlShards = properties.getShards().keySet();
                List<String> dbShards = topologyManager.getKnownShardsFromDb();

                // 3. Controlla se ci sono shard nuovi nello YAML o migrazioni interrotte da completare
                boolean hasNewShards = yamlShards.stream().anyMatch(s -> !dbShards.contains(s));
                List<TopologyManager.PendingMigration> pendingMigrations = topologyManager.getPendingMigrations();
                boolean hasPending = !pendingMigrations.isEmpty();

                if (hasNewShards || hasPending) {
                    if (hasPending) {
                        System.out.println("FRACTAL: Rilevate " + pendingMigrations.size() + " migrazioni interrotte da completare in modo idempotente.");
                    }
                    if (hasNewShards) {
                        System.out.println("FRACTAL: Rilevata discrepanza topologia. Nuovi shard aggiunti nello YAML!");
                    }

                    fractalRebalanceExecutor.execute(() -> {
                        if (topologyManager.tryAcquireRebalanceLock(
                                properties.getRebalancer().getLockTimeout(),
                                properties.getRebalancer().getLockRefreshInterval())) {
                            try {
                                if (properties.getRebalancer().getRootTable() == null || properties.getRebalancer().getRootIdColumn() == null) {
                                    System.out.println("FRACTAL: Rebalancer abilitato ma root-table o root-id-column non configurati. Registrazione shard.");
                                    yamlShards.forEach(s -> {
                                        topologyManager.registerNewShard(s);
                                        replicaTableSynchronizer.syncAllReplicaTablesToShard(s, properties.getRebalancer().getReplicaTables());
                                    });
                                    return;
                                }

                                // 1. Risoluzione idempotente di migrazioni interrotte da un precedente crash
                                if (!pendingMigrations.isEmpty()) {
                                    List<MigrationDeltaCalculator.MigrationAction> recoveryActions = pendingMigrations.stream()
                                            .map(p -> new MigrationDeltaCalculator.MigrationAction(p.tenantId(), p.sourceShard(), p.targetShard()))
                                            .toList();
                                    rebalanceEngine.executeMigration(recoveryActions);
                                }

                                // 2. Esecuzione del delta per i nuovi shard
                                if (hasNewShards) {
                                    MigrationDeltaCalculator calculator = new MigrationDeltaCalculator(
                                            buildDataSource(properties.getPrimary()),
                                            properties.getRebalancer()
                                    );
                                    List<MigrationDeltaCalculator.MigrationAction> actions =
                                            calculator.calculateDelta(dbShards, yamlShards, properties.getVirtualNodes());
                                    rebalanceEngine.executeMigration(actions);
                                    yamlShards.forEach(s -> {
                                        topologyManager.registerNewShard(s);
                                        replicaTableSynchronizer.syncAllReplicaTablesToShard(s, properties.getRebalancer().getReplicaTables());
                                    });
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

    /**
     * CONFIGURAZIONE OPZIONALE PER SPRING SECURITY
     * Questa classe interna viene processata SOLO SE Spring Security OAuth2 è presente nel classpath.
     * Altrimenti viene ignorata e non causa ClassNotFoundException.
     */
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