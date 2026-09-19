package neko.mukynas.fractal.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.Map;

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
        private String rootTable;
        private String rootIdColumn;
        private String statusColumn;
        private String migratingValue = "MIGRATING";
        private String activeValue = "ACTIVE";

        private List<String> shardedTables;
        private List<String> excludeTables;

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
    }
}