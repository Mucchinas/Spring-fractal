package neko.mukynas.fractal.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import neko.mukynas.fractal.aop.ShardingAspect;
import neko.mukynas.fractal.core.ConsistentHashRouter;
import neko.mukynas.fractal.core.ShardingKeyExtractor;
import neko.mukynas.fractal.datasource.ShardingRoutingDataSource;
import org.springframework.beans.factory.ObjectProvider;
import neko.mukynas.fractal.rebalance.MigrationDeltaCalculator;
import neko.mukynas.fractal.rebalance.RebalanceEngine;
import neko.mukynas.fractal.rebalance.TableDependencyResolver;
import neko.mukynas.fractal.rebalance.TopologyManager;
import org.springframework.boot.CommandLineRunner;
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
    public TopologyManager topologyManager(FractalProperties properties) {
        // Costruiamo il datasource primario appositamente per le logiche di admin
        DataSource primary = buildDataSource(properties.getPrimary());
        return new TopologyManager(primary);
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
        executor.initialize();
        return executor;
    }

    @Bean
    @ConditionalOnMissingBean
    public TableDependencyResolver tableDependencyResolver(FractalProperties properties) {
        DataSource primary = buildDataSource(properties.getPrimary());
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
    public CommandLineRunner fractalStartupListener(TopologyManager topologyManager,
                                                     TableDependencyResolver dependencyResolver,
                                                     RebalanceEngine rebalanceEngine,
                                                     ThreadPoolTaskExecutor fractalRebalanceExecutor,
                                                     FractalProperties properties) {
        return args -> {
            if (properties.getRebalancer().isEnabled()) {
                // 1. Crea le tabelle se non esistono
                topologyManager.initializeSchema();

                // 2. Legge dal file properties gli shard attuali
                Set<String> yamlShards = properties.getShards().keySet();
                List<String> dbShards = topologyManager.getKnownShardsFromDb();

                // 3. Controlla se ci sono shard nuovi nello YAML non presenti nel DB
                boolean hasNewShards = yamlShards.stream().anyMatch(s -> !dbShards.contains(s));

                if (hasNewShards) {
                    System.out.println("FRACTAL: Rilevata discrepanza topologia. Nuovi shard aggiunti nello YAML!");
                    fractalRebalanceExecutor.execute(() -> {
                        if (topologyManager.tryAcquireRebalanceLock()) {
                            try {
                                if (properties.getRebalancer().getRootTable() == null || properties.getRebalancer().getRootIdColumn() == null) {
                                    System.out.println("FRACTAL: Rebalancer abilitato ma root-table o root-id-column non configurati. Registrazione shard.");
                                    yamlShards.forEach(topologyManager::registerNewShard);
                                    return;
                                }
                                MigrationDeltaCalculator calculator = new MigrationDeltaCalculator(
                                        buildDataSource(properties.getPrimary()),
                                        properties.getRebalancer()
                                );
                                List<MigrationDeltaCalculator.MigrationAction> actions =
                                        calculator.calculateDelta(dbShards, yamlShards, properties.getVirtualNodes());
                                rebalanceEngine.executeMigration(actions);
                                yamlShards.forEach(topologyManager::registerNewShard);
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
            return new neko.mukynas.fractal.security.JwtSecurityKeyExtractor(properties.getJwt().getClaimName());
        }
    }
}