package io.github.mucchinas.fractal.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ConfigurationProperties(prefix = "fractal.sharding")
public class FractalProperties {

    private boolean enabled = true;
    private int virtualNodes = 150;
    private DataSourceProperties primary;
    private Map<String, DataSourceProperties> shards;
    private RebalancerProperties rebalancer = new RebalancerProperties();
    private JwtProperties jwt = new JwtProperties();

    public JwtProperties getJwt() { return jwt; }
    public void setJwt(JwtProperties jwt) { this.jwt = jwt; }

    public static class JwtProperties {
        private String claimName = "sub";

        public String getClaimName() {
            return claimName;
        }

        public void setClaimName(String claimName) {
            this.claimName = claimName;
        }
    }

    public RebalancerProperties getRebalancer() { return rebalancer; }
    public void setRebalancer(RebalancerProperties rebalancer) { this.rebalancer = rebalancer; }

    public static class RebalancerProperties {
        private boolean enabled = false;
        private boolean shardAll = false;
        private String rootTable;
        private String rootIdColumn;
        private String statusColumn;
        private String migratingValue = "MIGRATING";
        private String activeValue = "ACTIVE";

        private List<String> shardedTables;
        private List<String> replicaTables = new ArrayList<>();
        private List<String> excludeTables;
        private Duration lockTimeout = Duration.ofMinutes(15);
        private Duration lockRefreshInterval = Duration.ofMinutes(1);
        private Duration quiescencePeriod = Duration.ZERO;
        private Duration drainTimeout = Duration.ofSeconds(10);
        private Duration statusCacheTtl = Duration.ofSeconds(2);
        private long statusCacheMaxSize = 50_000L;
        private int batchSize = 500;
        private int maxBatchParameters = 32766;

        public Duration getStatusCacheTtl() {
            return statusCacheTtl;
        }

        public void setStatusCacheTtl(Duration statusCacheTtl) {
            this.statusCacheTtl = statusCacheTtl;
        }

        public long getStatusCacheMaxSize() {
            return statusCacheMaxSize;
        }

        public void setStatusCacheMaxSize(long statusCacheMaxSize) {
            this.statusCacheMaxSize = statusCacheMaxSize;
        }

        public List<String> getReplicaTables() {
            return replicaTables;
        }

        public void setReplicaTables(List<String> replicaTables) {
            this.replicaTables = replicaTables;
        }

        public Duration getLockTimeout() {
            return lockTimeout;
        }

        public void setLockTimeout(Duration lockTimeout) {
            this.lockTimeout = lockTimeout;
        }

        public Duration getLockRefreshInterval() {
            return lockRefreshInterval;
        }

        public void setLockRefreshInterval(Duration lockRefreshInterval) {
            this.lockRefreshInterval = lockRefreshInterval;
        }

        public boolean isShardAll() {
            return shardAll;
        }

        public void setShardAll(boolean shardAll) {
            this.shardAll = shardAll;
        }

        public List<String> getExcludeTables() {
            return excludeTables;
        }

        public void setExcludeTables(List<String> excludeTables) {
            this.excludeTables = excludeTables;
        }

        public List<String> getShardedTables() {
            return shardedTables;
        }

        public void setShardedTables(List<String> shardedTables) {
            this.shardedTables = shardedTables;
        }

        public String getActiveValue() {
            return activeValue;
        }

        public void setActiveValue(String activeValue) {
            this.activeValue = activeValue;
        }

        public String getMigratingValue() {
            return migratingValue;
        }

        public void setMigratingValue(String migratingValue) {
            this.migratingValue = migratingValue;
        }

        public String getStatusColumn() {
            return statusColumn;
        }

        public void setStatusColumn(String statusColumn) {
            this.statusColumn = statusColumn;
        }

        public String getRootIdColumn() {
            return rootIdColumn;
        }

        public void setRootIdColumn(String rootIdColumn) {
            this.rootIdColumn = rootIdColumn;
        }

        public String getRootTable() {
            return rootTable;
        }

        public void setRootTable(String rootTable) {
            this.rootTable = rootTable;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int calculateBatchSize(int columnCount) {
            if (columnCount <= 0) {
                return Math.max(1, batchSize);
            }
            int dynamicLimit = maxBatchParameters / columnCount;
            return Math.max(1, Math.min(batchSize, dynamicLimit));
        }

        public Duration getQuiescencePeriod() {
            return quiescencePeriod;
        }

        public void setQuiescencePeriod(Duration quiescencePeriod) {
            this.quiescencePeriod = quiescencePeriod;
        }

        public Duration getDrainTimeout() {
            return drainTimeout;
        }

        public void setDrainTimeout(Duration drainTimeout) {
            this.drainTimeout = drainTimeout;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getMaxBatchParameters() {
            return maxBatchParameters;
        }

        public void setMaxBatchParameters(int maxBatchParameters) {
            this.maxBatchParameters = maxBatchParameters;
        }
    }

    public DataSourceProperties getPrimary() {
        return primary;
    }

    public void setPrimary(DataSourceProperties primary) {
        this.primary = primary;
    }

    public Map<String, DataSourceProperties> getShards() {
        return shards;
    }

    public void setShards(Map<String, DataSourceProperties> shards) {
        this.shards = shards;
    }

    public Set<String> getActiveShardNames() {
        if (shards == null || shards.isEmpty()) {
            return Set.of();
        }
        return shards.entrySet().stream()
                .filter(entry -> entry.getValue() != null && !entry.getValue().isDecommission())
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }

    public Set<String> getDecommissioningShardNames() {
        if (shards == null || shards.isEmpty()) {
            return Set.of();
        }
        return shards.entrySet().stream()
                .filter(entry -> entry.getValue() != null && entry.getValue().isDecommission())
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }

    public boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getVirtualNodes() {
        return virtualNodes;
    }

    public void setVirtualNodes(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    public static class DataSourceProperties {

        private String jdbcUrl;
        private String username;
        private String password;
        private boolean initializeSchema = true;
        private boolean decommission = false;
        private boolean drain = false;
        private String status;

        public boolean isDrain() {
            return drain || isDecommission();
        }

        public void setDrain(boolean drain) {
            this.drain = drain;
        }

        public boolean isDecommission() {
            if (decommission) {
                return true;
            }
            if (status != null) {
                return "DRAINING".equalsIgnoreCase(status)
                        || "DECOMMISSIONING".equalsIgnoreCase(status)
                        || "DECOMMISSIONED".equalsIgnoreCase(status);
            }
            return false;
        }

        public void setDecommission(boolean decommission) {
            this.decommission = decommission;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public boolean isInitializeSchema() {
            return initializeSchema;
        }

        public void setInitializeSchema(boolean initializeSchema) {
            this.initializeSchema = initializeSchema;
        }
    }
}