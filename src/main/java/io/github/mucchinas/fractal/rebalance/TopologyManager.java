package io.github.mucchinas.fractal.rebalance;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.mucchinas.fractal.config.FractalProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TopologyManager implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(TopologyManager.class);

    public static final String REBALANCE_LOCK = "REBALANCE_LOCK";
    public static final String PHASE_PENDING = "PENDING";
    public static final String PHASE_COPYING = "COPYING";
    public static final String PHASE_PRUNING = "PRUNING";
    public static final String GLOBAL_REBALANCE_KEY = "GLOBAL_REBALANCE";
    public static final String PRIMARY_SHARD_NAME = "primary";
    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_DRAINING = "DRAINING";
    public static final String STATUS_DRAINED = "DRAINED";

    private final JdbcTemplate primaryJdbcTemplate;
    private final String instanceId;
    private final boolean autoInitializeSchema;
    private final Duration defaultLockTimeout;
    private final Set<String> activeMigratingTenants = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, java.util.concurrent.atomic.LongAdder> inFlightRequests = new ConcurrentHashMap<>();
    private final Cache<String, Boolean> migrationStatusCache;
    private final ConcurrentMap<String, String> pendingSourceShards = new ConcurrentHashMap<>();
    private final Cache<String, Boolean> rebalanceActiveCache;
    private volatile boolean rebalanceActive = false;

    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "fractal-lock-heartbeat");
        t.setDaemon(true);
        return t;
    });

    private ScheduledFuture<?> heartbeatTask;
    private volatile boolean lockHeld = false;
    private final AtomicInteger heartbeatFailures = new AtomicInteger(0);

    public TopologyManager(DataSource primaryDataSource) {
        this(primaryDataSource, true, Duration.ofSeconds(2), 50_000L, Duration.ofMinutes(15));
    }

    public TopologyManager(DataSource primaryDataSource, boolean autoInitializeSchema) {
        this(primaryDataSource, autoInitializeSchema, Duration.ofSeconds(2), 50_000L, Duration.ofMinutes(15));
    }

    public TopologyManager(DataSource primaryDataSource, boolean autoInitializeSchema, Duration statusCacheTtl, long statusCacheMaxSize) {
        this(primaryDataSource, autoInitializeSchema, statusCacheTtl, statusCacheMaxSize, Duration.ofMinutes(15));
    }

    public TopologyManager(DataSource primaryDataSource, boolean autoInitializeSchema, Duration statusCacheTtl, long statusCacheMaxSize, Duration lockTimeout) {
        this.primaryJdbcTemplate = new JdbcTemplate(primaryDataSource);
        this.instanceId = generateInstanceId();
        this.autoInitializeSchema = autoInitializeSchema;
        this.defaultLockTimeout = lockTimeout != null ? lockTimeout : Duration.ofMinutes(15);
        Duration ttl = statusCacheTtl != null ? statusCacheTtl : Duration.ofSeconds(2);
        long maxSize = statusCacheMaxSize > 0 ? statusCacheMaxSize : 50_000L;
        this.migrationStatusCache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(ttl)
                .build();
        this.rebalanceActiveCache = Caffeine.newBuilder()
                .maximumSize(1)
                .expireAfterWrite(ttl)
                .build();
    }

    @Override
    public void afterPropertiesSet() {
        if (autoInitializeSchema) {
            initializeSchema();
        }
    }

    public String getInstanceId() {
        return instanceId;
    }

    public boolean isLockHeld() {
        return lockHeld;
    }

    public Cache<String, Boolean> getMigrationStatusCache() {
        return migrationStatusCache;
    }

    public Cache<String, Boolean> getRebalanceActiveCache() {
        return rebalanceActiveCache;
    }

    public boolean isRebalanceActive() {
        if (rebalanceActive || !activeMigratingTenants.isEmpty() || !pendingSourceShards.isEmpty()) {
            return true;
        }
        return Boolean.TRUE.equals(rebalanceActiveCache.get(GLOBAL_REBALANCE_KEY, k -> checkGlobalRebalanceInDb()));
    }

    private boolean checkGlobalRebalanceInDb() {
        try {
            Duration timeout = defaultLockTimeout != null ? defaultLockTimeout : Duration.ofMinutes(15);
            Timestamp validThreshold = Timestamp.from(Instant.now().minus(timeout));
            List<String> locks = primaryJdbcTemplate.query(
                    "SELECT locked_by FROM fractal_locks WHERE lock_name = ? AND locked_at > ?",
                    (rs, rowNum) -> rs.getString(1), REBALANCE_LOCK, validThreshold);
            if (!locks.isEmpty()) {
                return true;
            }
            Integer count = primaryJdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM fractal_tenant_migrations", Integer.class);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }

    public void setRebalanceActive(boolean active) {
        this.rebalanceActive = active;
        rebalanceActiveCache.put(GLOBAL_REBALANCE_KEY, active);
        if (!active) {
            activeMigratingTenants.clear();
            pendingSourceShards.clear();
            migrationStatusCache.invalidateAll();
        }
    }

    public void clearActiveMigratingTenants() {
        activeMigratingTenants.clear();
    }

    public void registerPendingMigration(String tenantId, String sourceShard) {
        if (tenantId != null && sourceShard != null) {
            pendingSourceShards.put(tenantId, sourceShard);
            setRebalanceActive(true);
        }
    }

    public void registerPendingMigrations(Map<String, String> overrides) {
        if (overrides != null && !overrides.isEmpty()) {
            pendingSourceShards.putAll(overrides);
            setRebalanceActive(true);
        }
    }

    public String getPendingSourceShard(String tenantId) {
        if (tenantId == null) {
            return null;
        }
        String local = pendingSourceShards.get(tenantId);
        if (local != null) {
            return local;
        }
        try {
            List<String> list = primaryJdbcTemplate.query(
                    "SELECT source_shard FROM fractal_tenant_migrations WHERE tenant_id = ? AND phase = ?",
                    (rs, rowNum) -> rs.getString(1), tenantId, PHASE_PENDING);
            if (!list.isEmpty()) {
                return list.get(0);
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    public void completePendingMigration(String tenantId) {
        if (tenantId != null) {
            pendingSourceShards.remove(tenantId);
            clearMigrationRecord(tenantId);
        }
    }

    public void clearPendingMigrations() {
        pendingSourceShards.clear();
    }

    public void recordPendingMigration(String tenantId, String sourceShard, String targetShard) {
        try {
            List<String> existingPhases = primaryJdbcTemplate.query(
                    "SELECT phase FROM fractal_tenant_migrations WHERE tenant_id = ?",
                    (rs, rowNum) -> rs.getString(1), tenantId);
            if (existingPhases.isEmpty()) {
                registerPendingMigration(tenantId, sourceShard);
                primaryJdbcTemplate.update("""
                    INSERT INTO fractal_tenant_migrations (tenant_id, source_shard, target_shard, phase, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                """, tenantId, sourceShard, targetShard, PHASE_PENDING, Timestamp.from(Instant.now()));
            } else if (PHASE_PENDING.equalsIgnoreCase(existingPhases.get(0))) {
                registerPendingMigration(tenantId, sourceShard);
            }
        } catch (Exception ignored) {
            registerPendingMigration(tenantId, sourceShard);
        }
    }

    public void markTenantMigrating(String tenantId) {
        if (tenantId != null) {
            activeMigratingTenants.add(tenantId);
            migrationStatusCache.put(tenantId, true);
        }
    }

    public void markTenantActive(String tenantId) {
        if (tenantId != null) {
            activeMigratingTenants.remove(tenantId);
            migrationStatusCache.put(tenantId, false);
        }
    }

    public void registerRequestStart(String tenantId) {
        if (tenantId != null) {
            inFlightRequests.computeIfAbsent(tenantId, k -> new java.util.concurrent.atomic.LongAdder()).increment();
        }
    }

    public void registerRequestEnd(String tenantId) {
        if (tenantId != null) {
            inFlightRequests.computeIfPresent(tenantId, (k, adder) -> {
                adder.decrement();
                return adder.sum() <= 0 ? null : adder;
            });
        }
    }

    public long getInFlightRequestCount(String tenantId) {
        if (tenantId == null) return 0;
        java.util.concurrent.atomic.LongAdder adder = inFlightRequests.get(tenantId);
        return adder != null ? Math.max(0, adder.sum()) : 0;
    }

    public boolean awaitTenantQuiescence(String tenantId, Duration timeout) {
        if (tenantId == null) return true;
        long timeoutMillis = timeout != null ? timeout.toMillis() : 5000;
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            java.util.concurrent.atomic.LongAdder adder = inFlightRequests.get(tenantId);
            if (adder == null || adder.sum() <= 0) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        java.util.concurrent.atomic.LongAdder adder = inFlightRequests.get(tenantId);
        return adder == null || adder.sum() <= 0;
    }

    public boolean isTenantMigrating(String tenantId) {
        FractalProperties.RebalancerProperties props = new FractalProperties.RebalancerProperties();
        props.setEnabled(true);
        return isTenantMigrating(tenantId, props);
    }

    public boolean isTenantMigrating(String tenantId, FractalProperties.RebalancerProperties props) {
        if (tenantId == null || props == null || !props.isEnabled()) {
            return false;
        }

        if (activeMigratingTenants.contains(tenantId)) {
            return true;
        }

        return Boolean.TRUE.equals(migrationStatusCache.get(tenantId, key -> checkMigrationInPrimaryDb(key, props)));
    }

    public boolean checkMigrationInPrimaryDb(String tenantId) {
        return checkMigrationInPrimaryDb(tenantId, null);
    }

    public boolean checkMigrationInPrimaryDb(String tenantId, FractalProperties.RebalancerProperties props) {
        if (tenantId == null) {
            return false;
        }
        try {
            List<String> phases = primaryJdbcTemplate.query(
                    "SELECT phase FROM fractal_tenant_migrations WHERE tenant_id = ?",
                    (rs, rowNum) -> rs.getString(1), tenantId);
            if (!phases.isEmpty()) {
                String phase = phases.get(0);
                if (PHASE_COPYING.equalsIgnoreCase(phase) || PHASE_PRUNING.equalsIgnoreCase(phase)) {
                    return true;
                }
            }
        } catch (Exception ignored) {
        }

        if (props != null && props.getRootTable() != null && props.getRootIdColumn() != null && props.getStatusColumn() != null) {
            try {
                String sql = String.format("SELECT %s FROM %s WHERE %s = ?",
                        props.getStatusColumn(), props.getRootTable(), props.getRootIdColumn());
                List<String> statuses = primaryJdbcTemplate.query(sql, (rs, rowNum) -> rs.getString(1), tenantId);
                if (!statuses.isEmpty() && props.getMigratingValue() != null) {
                    return props.getMigratingValue().equalsIgnoreCase(statuses.get(0));
                }
            } catch (Exception e) {
                return false;
            }
        }

        return false;
    }

    public void initializeSchema() {
        primaryJdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS fractal_shard_topology (
                shard_name VARCHAR(255) PRIMARY KEY,
                status VARCHAR(50) NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);
        primaryJdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS fractal_locks (
                lock_name VARCHAR(255) PRIMARY KEY,
                locked_by VARCHAR(255) NOT NULL,
                locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);
        primaryJdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS fractal_tenant_migrations (
                tenant_id VARCHAR(255) PRIMARY KEY,
                source_shard VARCHAR(255) NOT NULL,
                target_shard VARCHAR(255) NOT NULL,
                phase VARCHAR(50) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);
    }

    public List<String> getKnownShardsFromDb() {
        return primaryJdbcTemplate.queryForList(
                "SELECT shard_name FROM fractal_shard_topology WHERE shard_name != ?", String.class, PRIMARY_SHARD_NAME);
    }

    public List<String> getActiveShardsFromDb() {
        return primaryJdbcTemplate.queryForList(
                "SELECT shard_name FROM fractal_shard_topology WHERE status = 'ACTIVE' AND shard_name != ?", String.class, PRIMARY_SHARD_NAME);
    }

    public List<String> getDrainingShardsFromDb() {
        return primaryJdbcTemplate.queryForList(
                "SELECT shard_name FROM fractal_shard_topology WHERE status = 'DRAINING' AND shard_name != ?", String.class, PRIMARY_SHARD_NAME);
    }

    public boolean isPrimaryDrained() {
        try {
            List<String> statuses = primaryJdbcTemplate.query(
                    "SELECT status FROM fractal_shard_topology WHERE shard_name = ?",
                    (rs, rowNum) -> rs.getString(1), PRIMARY_SHARD_NAME);
            return !statuses.isEmpty() && STATUS_DRAINED.equalsIgnoreCase(statuses.get(0));
        } catch (Exception e) {
            return false;
        }
    }

    public void markPrimaryDraining() {
        try {
            List<String> existing = primaryJdbcTemplate.query(
                    "SELECT shard_name FROM fractal_shard_topology WHERE shard_name = ?",
                    (rs, rowNum) -> rs.getString(1), PRIMARY_SHARD_NAME);
            if (existing.isEmpty()) {
                primaryJdbcTemplate.update(
                        "INSERT INTO fractal_shard_topology (shard_name, status) VALUES (?, ?)",
                        PRIMARY_SHARD_NAME, STATUS_DRAINING);
            } else {
                primaryJdbcTemplate.update(
                        "UPDATE fractal_shard_topology SET status = ? WHERE shard_name = ?",
                        STATUS_DRAINING, PRIMARY_SHARD_NAME);
            }
        } catch (Exception ignored) {
        }
    }

    public void markPrimaryDrained() {
        try {
            List<String> existing = primaryJdbcTemplate.query(
                    "SELECT shard_name FROM fractal_shard_topology WHERE shard_name = ?",
                    (rs, rowNum) -> rs.getString(1), PRIMARY_SHARD_NAME);
            if (existing.isEmpty()) {
                primaryJdbcTemplate.update(
                        "INSERT INTO fractal_shard_topology (shard_name, status) VALUES (?, ?)",
                        PRIMARY_SHARD_NAME, STATUS_DRAINED);
            } else {
                primaryJdbcTemplate.update(
                        "UPDATE fractal_shard_topology SET status = ? WHERE shard_name = ?",
                        STATUS_DRAINED, PRIMARY_SHARD_NAME);
            }
        } catch (Exception ignored) {
        }
    }

    public void markShardDraining(String shardName) {
        if (shardName != null) {
            try {
                primaryJdbcTemplate.update(
                        "UPDATE fractal_shard_topology SET status = 'DRAINING' WHERE shard_name = ?",
                        shardName
                );
            } catch (Exception ignored) {
            }
        }
    }

    public void removeShard(String shardName) {
        if (shardName != null) {
            try {
                primaryJdbcTemplate.update(
                        "DELETE FROM fractal_shard_topology WHERE shard_name = ?",
                        shardName
                );
            } catch (Exception ignored) {
            }
        }
    }

    public boolean hasPendingMigrationsForShard(String shardName) {
        if (shardName == null) return false;
        try {
            Integer count = primaryJdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM fractal_tenant_migrations WHERE source_shard = ?",
                    Integer.class, shardName);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }

    public void registerNewShard(String shardName) {
        List<String> existing = primaryJdbcTemplate.query(
                "SELECT shard_name FROM fractal_shard_topology WHERE shard_name = ?",
                (rs, rowNum) -> rs.getString(1), shardName);
        if (existing.isEmpty()) {
            try {
                primaryJdbcTemplate.update(
                        "INSERT INTO fractal_shard_topology (shard_name, status) VALUES (?, 'ACTIVE')",
                        shardName
                );
            } catch (Exception ignored) {
            }
        } else {
            try {
                primaryJdbcTemplate.update(
                        "UPDATE fractal_shard_topology SET status = 'ACTIVE' WHERE shard_name = ?",
                        shardName
                );
            } catch (Exception ignored) {
            }
        }
    }

    public boolean tryAcquireRebalanceLock() {
        return tryAcquireRebalanceLock(Duration.ofMinutes(15), Duration.ofMinutes(1));
    }

    public synchronized boolean tryAcquireRebalanceLock(Duration lockTimeout, Duration lockRefreshInterval) {
        Instant now = Instant.now();
        try {
            int inserted = primaryJdbcTemplate.update(
                    "INSERT INTO fractal_locks (lock_name, locked_by, locked_at) VALUES (?, ?, ?)",
                    REBALANCE_LOCK, instanceId, Timestamp.from(now)
            );
            if (inserted > 0) {
                lockHeld = true;
                setRebalanceActive(true);
                startHeartbeat(lockRefreshInterval);
                return true;
            }
        } catch (Exception ignored) {
        }
        long timeoutMs = lockTimeout != null ? lockTimeout.toMillis() : 900_000L;
        Timestamp expirationThreshold = Timestamp.from(now.minusMillis(timeoutMs));

        int updated = primaryJdbcTemplate.update("""
            UPDATE fractal_locks
            SET locked_by = ?, locked_at = ?
            WHERE lock_name = ? AND locked_at < ?
        """, instanceId, Timestamp.from(now), REBALANCE_LOCK, expirationThreshold);

        if (updated > 0) {
            lockHeld = true;
            setRebalanceActive(true);
            startHeartbeat(lockRefreshInterval);
            return true;
        }

        return false;
    }

    public synchronized void releaseRebalanceLock() {
        stopHeartbeat();
        try {
            primaryJdbcTemplate.update(
                    "DELETE FROM fractal_locks WHERE lock_name = ? AND locked_by = ?",
                    REBALANCE_LOCK, instanceId
            );
        } catch (Exception e) {
            log.error("FRACTAL: Error releasing rebalance lock: {}", e.getMessage(), e);
        } finally {
            lockHeld = false;
            heartbeatFailures.set(0);
            setRebalanceActive(false);
            clearPendingMigrations();
        }
    }

    private synchronized void startHeartbeat(Duration refreshInterval) {
        stopHeartbeat();
        heartbeatFailures.set(0);
        long intervalMs = refreshInterval != null ? refreshInterval.toMillis() : 60_000L;
        heartbeatTask = heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                int updated = primaryJdbcTemplate.update(
                        "UPDATE fractal_locks SET locked_at = ? WHERE lock_name = ? AND locked_by = ?",
                        Timestamp.from(Instant.now()), REBALANCE_LOCK, instanceId
                );
                if (updated > 0) {
                    heartbeatFailures.set(0);
                } else {
                    int fails = heartbeatFailures.incrementAndGet();
                    log.warn("FRACTAL: Heartbeat lock update affected 0 rows (attempt {})", fails);
                    if (fails >= 3) {
                        lockHeld = false;
                        log.error("FRACTAL: Rebalance lock was lost or taken over. Relinquishing local lock.");
                    }
                }
            } catch (Exception e) {
                int fails = heartbeatFailures.incrementAndGet();
                log.error("FRACTAL: Failed to refresh rebalance lock heartbeat (failure {}): {}", fails, e.getMessage());
                if (fails >= 3) {
                    lockHeld = false;
                    log.error("FRACTAL: Heartbeat failed 3 consecutive times. Relinquishing local lock to prevent split-brain.");
                }
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private synchronized void stopHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(true);
            heartbeatTask = null;
        }
    }

    public void recordMigrationStart(String tenantId, String sourceShard, String targetShard) {
        if (tenantId != null) {
            migrationStatusCache.put(tenantId, true);
        }
        try {
            primaryJdbcTemplate.update("DELETE FROM fractal_tenant_migrations WHERE tenant_id = ?", tenantId);
            primaryJdbcTemplate.update("""
                INSERT INTO fractal_tenant_migrations (tenant_id, source_shard, target_shard, phase, updated_at)
                VALUES (?, ?, ?, ?, ?)
            """, tenantId, sourceShard, targetShard, PHASE_COPYING, Timestamp.from(Instant.now()));
        } catch (Exception ignored) {
        }
    }

    public void updateMigrationPhase(String tenantId, String phase) {
        try {
            primaryJdbcTemplate.update("""
                UPDATE fractal_tenant_migrations
                SET phase = ?, updated_at = ?
                WHERE tenant_id = ?
            """, phase, Timestamp.from(Instant.now()), tenantId);
        } catch (Exception ignored) {
        }
    }

    public void clearMigrationRecord(String tenantId) {
        if (tenantId != null) {
            migrationStatusCache.invalidate(tenantId);
        }
        try {
            primaryJdbcTemplate.update(
                    "DELETE FROM fractal_tenant_migrations WHERE tenant_id = ?",
                    tenantId
            );
        } catch (Exception ignored) {
        }
    }

    public String getMigrationPhase(String tenantId) {
        try {
            List<String> phases = primaryJdbcTemplate.query(
                    "SELECT phase FROM fractal_tenant_migrations WHERE tenant_id = ?",
                    (rs, rowNum) -> rs.getString(1), tenantId
            );
            return phases.isEmpty() ? null : phases.get(0);
        } catch (Exception e) {
            return null;
        }
    }

    public List<PendingMigration> getPendingMigrations() {
        try {
            return primaryJdbcTemplate.query("""
                SELECT tenant_id, source_shard, target_shard, phase
                FROM fractal_tenant_migrations
            """, (rs, rowNum) -> new PendingMigration(
                    rs.getString("tenant_id"),
                    rs.getString("source_shard"),
                    rs.getString("target_shard"),
                    rs.getString("phase")
            ));
        } catch (Exception e) {
            return List.of();
        }
    }

    @Override
    public void destroy() {
        stopHeartbeat();
        heartbeatExecutor.shutdownNow();
        if (lockHeld) {
            releaseRebalanceLock();
        }
    }

    private String generateInstanceId() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + System.currentTimeMillis();
        } catch (Exception e) {
            return "instance-" + System.currentTimeMillis();
        }
    }

    public record PendingMigration(String tenantId, String sourceShard, String targetShard, String phase) {}
}