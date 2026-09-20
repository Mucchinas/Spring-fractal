package io.github.mucchinas.fractal.rebalance;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.mucchinas.fractal.config.FractalProperties;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Manages shard topology, distributed locking with TTL/heartbeat,
 * and tracks in-flight tenant migrations for idempotent crash recovery.
 */
public class TopologyManager implements InitializingBean, DisposableBean {

    public static final String REBALANCE_LOCK = "REBALANCE_LOCK";
    public static final String PHASE_COPYING = "COPYING";
    public static final String PHASE_PRUNING = "PRUNING";

    private final JdbcTemplate primaryJdbcTemplate;
    private final String instanceId;
    private final boolean autoInitializeSchema;
    private final Set<String> activeMigratingTenants = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, java.util.concurrent.atomic.LongAdder> inFlightRequests = new ConcurrentHashMap<>();
    private final Cache<String, Boolean> migrationStatusCache;

    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "fractal-lock-heartbeat");
        t.setDaemon(true);
        return t;
    });

    private ScheduledFuture<?> heartbeatTask;
    private volatile boolean lockHeld = false;

    public TopologyManager(DataSource primaryDataSource) {
        this(primaryDataSource, true, Duration.ofSeconds(2), 50_000L);
    }

    public TopologyManager(DataSource primaryDataSource, boolean autoInitializeSchema) {
        this(primaryDataSource, autoInitializeSchema, Duration.ofSeconds(2), 50_000L);
    }

    public TopologyManager(DataSource primaryDataSource, boolean autoInitializeSchema, Duration statusCacheTtl, long statusCacheMaxSize) {
        this.primaryJdbcTemplate = new JdbcTemplate(primaryDataSource);
        this.instanceId = generateInstanceId();
        this.autoInitializeSchema = autoInitializeSchema;
        Duration ttl = statusCacheTtl != null ? statusCacheTtl : Duration.ofSeconds(2);
        long maxSize = statusCacheMaxSize > 0 ? statusCacheMaxSize : 50_000L;
        this.migrationStatusCache = Caffeine.newBuilder()
                .maximumSize(maxSize)
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
            java.util.concurrent.atomic.LongAdder adder = inFlightRequests.get(tenantId);
            if (adder != null) {
                adder.decrement();
            }
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

    public boolean isTenantMigrating(String tenantId, FractalProperties.RebalancerProperties props) {
        if (tenantId == null || props == null || !props.isEnabled()) {
            return false;
        }

        if (activeMigratingTenants.contains(tenantId)) {
            return true;
        }

        return Boolean.TRUE.equals(migrationStatusCache.get(tenantId, key -> checkMigrationInPrimaryDb(key, props)));
    }

    private boolean checkMigrationInPrimaryDb(String tenantId, FractalProperties.RebalancerProperties props) {
        // Check if there is an in-flight migration record in fractal_tenant_migrations
        try {
            List<String> phases = primaryJdbcTemplate.query(
                    "SELECT phase FROM fractal_tenant_migrations WHERE tenant_id = ?",
                    (rs, rowNum) -> rs.getString(1), tenantId);
            if (!phases.isEmpty()) {
                return true;
            }
        } catch (Exception ignored) {
        }

        if (props.getRootTable() != null && props.getRootIdColumn() != null && props.getStatusColumn() != null) {
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
        // 1. Shard Topology Table
        primaryJdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS fractal_shard_topology (
                shard_name VARCHAR(255) PRIMARY KEY,
                status VARCHAR(50) NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);

        // 2. Distributed Locks Table with Timestamp
        primaryJdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS fractal_locks (
                lock_name VARCHAR(255) PRIMARY KEY,
                locked_by VARCHAR(255) NOT NULL,
                locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);

        // 3. In-flight Tenant Migrations Table (for crash recovery & idempotent retries)
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
                "SELECT shard_name FROM fractal_shard_topology", String.class);
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
                // Ignore concurrent insert collision from another node
            }
        }
    }

    /**
     * Attempts to acquire the rebalance lock using default 15-minute TTL and 1-minute refresh interval.
     */
    public boolean tryAcquireRebalanceLock() {
        return tryAcquireRebalanceLock(Duration.ofMinutes(15), Duration.ofMinutes(1));
    }

    /**
     * Attempts to acquire the rebalance lock with configurable timeout and refresh interval.
     * If the lock is already held, checks if it has expired beyond lockTimeout.
     * If expired (due to a power outage or crash of another pod), atomically takes it over.
     */
    public synchronized boolean tryAcquireRebalanceLock(Duration lockTimeout, Duration lockRefreshInterval) {
        Instant now = Instant.now();

        // 1. Try atomic INSERT
        try {
            int inserted = primaryJdbcTemplate.update(
                    "INSERT INTO fractal_locks (lock_name, locked_by, locked_at) VALUES (?, ?, ?)",
                    REBALANCE_LOCK, instanceId, Timestamp.from(now)
            );
            if (inserted > 0) {
                lockHeld = true;
                startHeartbeat(lockRefreshInterval);
                return true;
            }
        } catch (Exception ignored) {
            // Row already exists; proceed to expiration check
        }

        // 2. Check and atomically take over expired stale lock
        long timeoutMs = lockTimeout != null ? lockTimeout.toMillis() : 900_000L;
        Timestamp expirationThreshold = Timestamp.from(now.minusMillis(timeoutMs));

        int updated = primaryJdbcTemplate.update("""
            UPDATE fractal_locks
            SET locked_by = ?, locked_at = ?
            WHERE lock_name = ? AND locked_at < ?
        """, instanceId, Timestamp.from(now), REBALANCE_LOCK, expirationThreshold);

        if (updated > 0) {
            lockHeld = true;
            startHeartbeat(lockRefreshInterval);
            return true;
        }

        return false;
    }

    /**
     * Releases the rebalance lock if held by this instance and stops the heartbeat.
     */
    public synchronized void releaseRebalanceLock() {
        stopHeartbeat();
        try {
            primaryJdbcTemplate.update(
                    "DELETE FROM fractal_locks WHERE lock_name = ? AND locked_by = ?",
                    REBALANCE_LOCK, instanceId
            );
        } catch (Exception e) {
            System.err.println("FRACTAL: Error releasing rebalance lock: " + e.getMessage());
        } finally {
            lockHeld = false;
        }
    }

    /**
     * Periodically refreshes the lock timestamp to prevent expiration during long migrations.
     */
    private synchronized void startHeartbeat(Duration refreshInterval) {
        stopHeartbeat();
        long intervalMs = refreshInterval != null ? refreshInterval.toMillis() : 60_000L;
        heartbeatTask = heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                primaryJdbcTemplate.update(
                        "UPDATE fractal_locks SET locked_at = ? WHERE lock_name = ? AND locked_by = ?",
                        Timestamp.from(Instant.now()), REBALANCE_LOCK, instanceId
                );
            } catch (Exception e) {
                System.err.println("FRACTAL: Failed to refresh rebalance lock heartbeat: " + e.getMessage());
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private synchronized void stopHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(true);
            heartbeatTask = null;
        }
    }

    // In-flight Migration Tracking for Idempotent Crash Recovery

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