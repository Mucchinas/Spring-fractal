package neko.mukynas.fractal.rebalance;

import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;

public class TopologyManager {

    private final JdbcTemplate primaryJdbcTemplate;
    private final String instanceId;
    private final java.util.Set<String> activeMigratingTenants = java.util.concurrent.ConcurrentHashMap.newKeySet();

    public TopologyManager(DataSource primaryDataSource) {
        this.primaryJdbcTemplate = new JdbcTemplate(primaryDataSource);
        this.instanceId = generateInstanceId();
    }

    public void markTenantMigrating(String tenantId) {
        if (tenantId != null) {
            activeMigratingTenants.add(tenantId);
        }
    }

    public void markTenantActive(String tenantId) {
        if (tenantId != null) {
            activeMigratingTenants.remove(tenantId);
        }
    }

    public boolean isTenantMigrating(String tenantId, neko.mukynas.fractal.config.FractalProperties.RebalancerProperties props) {
        if (tenantId == null || props == null || !props.isEnabled()) {
            return false;
        }

        if (activeMigratingTenants.contains(tenantId)) {
            return true;
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
                // If table doesn't exist yet or query fails, fall back to in-memory status
                return false;
            }
        }

        return false;
    }

    public void initializeSchema() {
        // 1. Tabella Topologia
        primaryJdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS fractal_shard_topology (
                shard_name VARCHAR(255) PRIMARY KEY,
                status VARCHAR(50) NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);

        // 2. Tabella Lock Distribuiti
        primaryJdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS fractal_locks (
                lock_name VARCHAR(255) PRIMARY KEY,
                locked_by VARCHAR(255) NOT NULL,
                locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);
    }

    /**
     * Legge gli shard noti dal database
     */
    public List<String> getKnownShardsFromDb() {
        return primaryJdbcTemplate.queryForList(
                "SELECT shard_name FROM fractal_shard_topology", String.class);
    }

    /**
     * Registra un nuovo shard nel DB
     */
    public void registerNewShard(String shardName) {
        primaryJdbcTemplate.update(
                "INSERT INTO fractal_shard_topology (shard_name, status) VALUES (?, 'ACTIVE') ON CONFLICT DO NOTHING",
                shardName
        );
    }

    /**
     * Tenta di acquisire il lock per il rebalancing.
     * Sfrutta l'univocità della Primary Key per evitare race conditions tra Pod multipli.
     */
    public boolean tryAcquireRebalanceLock() {
        try {
            int rows = primaryJdbcTemplate.update(
                    "INSERT INTO fractal_locks (lock_name, locked_by) VALUES ('REBALANCE_LOCK', ?)",
                    instanceId
            );
            return rows > 0;
        } catch (Exception e) {
            // Se la query fallisce (violazione Primary Key), un altro pod ha il lock.
            return false;
        }
    }

    public void releaseRebalanceLock() {
        primaryJdbcTemplate.update("DELETE FROM fractal_locks WHERE lock_name = 'REBALANCE_LOCK' AND locked_by = ?", instanceId);
    }

    private String generateInstanceId() {
        try {
            // Identifica univocamente questo pod (hostname + thread)
            return InetAddress.getLocalHost().getHostName() + "-" + Thread.currentThread().getId();
        } catch (Exception e) {
            return "unknown-instance-" + System.currentTimeMillis();
        }
    }
}