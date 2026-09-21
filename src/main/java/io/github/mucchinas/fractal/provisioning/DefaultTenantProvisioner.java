package io.github.mucchinas.fractal.provisioning;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.core.Authentication;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultTenantProvisioner implements TenantProvisioner {

    private static final Logger log = LoggerFactory.getLogger(DefaultTenantProvisioner.class);

    private final DataSource primaryDataSource;
    private final Map<String, DataSource> shardDataSources;
    private final ConsistentHashRouter router;
    private final FractalProperties properties;
    private final RootEntityAttributeExtractor attributeExtractor;
    private final ObjectProvider<TenantInitializer> tenantInitializers;
    private final ObjectProvider<RootEntityCustomizer> rootEntityCustomizers;

    private final ConcurrentMap<String, Object> tenantLocks = new ConcurrentHashMap<>();
    private final Cache<String, Boolean> knownTenantsCache;

    public DefaultTenantProvisioner(DataSource primaryDataSource,
                                    Map<String, DataSource> shardDataSources,
                                    ConsistentHashRouter router,
                                    FractalProperties properties,
                                    RootEntityAttributeExtractor attributeExtractor,
                                    ObjectProvider<TenantInitializer> tenantInitializers,
                                    ObjectProvider<RootEntityCustomizer> rootEntityCustomizers) {
        this.primaryDataSource = primaryDataSource;
        this.shardDataSources = shardDataSources != null ? shardDataSources : Collections.emptyMap();
        this.router = router;
        this.properties = properties;
        this.attributeExtractor = attributeExtractor;
        this.tenantInitializers = tenantInitializers;
        this.rootEntityCustomizers = rootEntityCustomizers;

        Duration ttl = (properties != null && properties.getRebalancer() != null && properties.getRebalancer().getStatusCacheTtl() != null)
                ? properties.getRebalancer().getStatusCacheTtl()
                : Duration.ofMinutes(10);
        long maxSize = (properties != null && properties.getRebalancer() != null && properties.getRebalancer().getStatusCacheMaxSize() > 0)
                ? properties.getRebalancer().getStatusCacheMaxSize()
                : 50_000L;

        this.knownTenantsCache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(ttl)
                .build();
    }

    @Override
    public ProvisioningResult ensureProvisioned(String tenantId, Authentication authentication) {
        return doProvision(tenantId, authentication, null);
    }

    @Override
    public ProvisioningResult provisionIfAbsent(String tenantId, Map<String, Object> customAttributes) {
        return doProvision(tenantId, null, customAttributes);
    }

    private ProvisioningResult doProvision(String tenantId, Authentication auth, Map<String, Object> customAttributes) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("Fractal Provisioning: Tenant ID cannot be null or blank.");
        }

        String targetShard = resolveTargetShard(tenantId);
        if (targetShard == null) {
            throw new IllegalStateException("Fractal Provisioning: No active shard available for tenant " + tenantId);
        }

        // Fast-path in-memory check (~15ns)
        if (Boolean.TRUE.equals(knownTenantsCache.getIfPresent(tenantId))) {
            return new ProvisioningResult(tenantId, targetShard, false);
        }

        Object lock = tenantLocks.computeIfAbsent(tenantId, k -> new Object());
        synchronized (lock) {
            try {
                if (Boolean.TRUE.equals(knownTenantsCache.getIfPresent(tenantId))) {
                    return new ProvisioningResult(tenantId, targetShard, false);
                }

                boolean onPrimary = existsOnPrimary(tenantId);
                boolean onShard = existsOnShard(tenantId);

                if (onPrimary && onShard) {
                    knownTenantsCache.put(tenantId, true);
                    return new ProvisioningResult(tenantId, targetShard, false);
                }

                Map<String, Object> columns = attributeExtractor.extractAttributes(tenantId, auth, customAttributes);

                if (rootEntityCustomizers != null) {
                    for (RootEntityCustomizer customizer : rootEntityCustomizers.orderedStream().toList()) {
                        customizer.customize(tenantId, columns, auth);
                    }
                }

                String rootTable = properties.getRebalancer().getRootTable();
                if (rootTable == null) {
                    throw new IllegalStateException("Fractal Provisioning: rootTable not configured; cannot create root record.");
                }

                if (!onPrimary) {
                    insertRootRow(primaryDataSource, rootTable, columns, tenantId);
                    log.info("FRACTAL Provisioning: Created root record on primary database for tenant {}", tenantId);
                }

                if (!onShard) {
                    DataSource shardDs = shardDataSources.get(targetShard);
                    if (shardDs != null) {
                        insertRootRow(shardDs, rootTable, columns, tenantId);
                        log.info("FRACTAL Provisioning: Created root record on shard {} for tenant {}", targetShard, tenantId);
                    }
                }

                if (tenantInitializers != null) {
                    String prevShard = ShardContextHolder.getShard();
                    try {
                        ShardContextHolder.setShard(targetShard);
                        for (TenantInitializer initializer : tenantInitializers.orderedStream().toList()) {
                            initializer.initializeTenant(tenantId, targetShard, Collections.unmodifiableMap(columns));
                        }
                    } finally {
                        if (prevShard != null) {
                            ShardContextHolder.setShard(prevShard);
                        } else {
                            ShardContextHolder.clear();
                        }
                    }
                }

                knownTenantsCache.put(tenantId, true);
                return new ProvisioningResult(tenantId, targetShard, true);
            } finally {
                tenantLocks.remove(tenantId, lock);
            }
        }
    }

    private void insertRootRow(DataSource ds, String rootTable, Map<String, Object> columns, String tenantId) {
        if (ds == null || columns == null || columns.isEmpty()) {
            return;
        }

        StringJoiner colNames = new StringJoiner(", ");
        StringJoiner paramNames = new StringJoiner(", ");
        MapSqlParameterSource params = new MapSqlParameterSource();

        for (Map.Entry<String, Object> entry : columns.entrySet()) {
            colNames.add(entry.getKey());
            paramNames.add(":" + entry.getKey());
            params.addValue(entry.getKey(), entry.getValue());
        }

        String sql = "INSERT INTO " + rootTable + " (" + colNames + ") VALUES (" + paramNames + ")";
        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(ds);

        try {
            template.update(sql, params);
        } catch (DataIntegrityViolationException e) {
            log.debug("FRACTAL Provisioning: Root record for tenant {} already exists (concurrent insert): {}", tenantId, e.getMessage());
        }
    }

    @Override
    public boolean existsOnPrimary(String tenantId) {
        String rootTable = properties.getRebalancer().getRootTable();
        String rootIdColumn = properties.getRebalancer().getRootIdColumn();
        if (primaryDataSource == null || rootTable == null || rootIdColumn == null) {
            return false;
        }
        String sql = "SELECT COUNT(*) FROM " + rootTable + " WHERE " + rootIdColumn + " = ?";
        try {
            Integer count = new JdbcTemplate(primaryDataSource).queryForObject(sql, Integer.class, tenantId);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean existsOnShard(String tenantId) {
        String targetShard = resolveTargetShard(tenantId);
        if (targetShard == null) {
            return false;
        }
        DataSource shardDs = shardDataSources.get(targetShard);
        if (shardDs == null) {
            return false;
        }
        String rootTable = properties.getRebalancer().getRootTable();
        String rootIdColumn = properties.getRebalancer().getRootIdColumn();
        if (rootTable == null || rootIdColumn == null) {
            return false;
        }
        String sql = "SELECT COUNT(*) FROM " + rootTable + " WHERE " + rootIdColumn + " = ?";
        try {
            Integer count = new JdbcTemplate(shardDs).queryForObject(sql, Integer.class, tenantId);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String resolveTargetShard(String tenantId) {
        return router != null ? router.routeNode(tenantId) : null;
    }
}
