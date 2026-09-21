package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.config.FractalProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.*;

public class PhysicalSchemaAuditValidator {

    private static final Logger log = LoggerFactory.getLogger(PhysicalSchemaAuditValidator.class);

    private final Map<String, DataSource> shardDataSources;
    private final FractalProperties properties;
    private final TableDependencyResolver dependencyResolver;

    public PhysicalSchemaAuditValidator(Map<String, DataSource> shardDataSources,
                                        FractalProperties properties,
                                        TableDependencyResolver dependencyResolver) {
        this.shardDataSources = shardDataSources != null ? shardDataSources : Collections.emptyMap();
        this.properties = properties != null ? properties : new FractalProperties();
        this.dependencyResolver = dependencyResolver;
    }

    public void validate() {
        FractalProperties.ValidationProperties validation = properties.getValidation();
        FractalProperties.ValidationProperties.EnforcementMode mode = validation.getSchemaAuditAction();
        if (mode == FractalProperties.ValidationProperties.EnforcementMode.DISABLED) {
            return;
        }

        String rootTable = properties.getRebalancer().getRootTable();
        if (rootTable == null || rootTable.isBlank()) {
            return;
        }
        String normRoot = rootTable.toLowerCase().trim();

        List<TableForeignKey> allFks = dependencyResolver != null ? dependencyResolver.loadForeignKeys() : Collections.emptyList();

        Set<String> exclusions = new HashSet<>(Arrays.asList(
                "fractal_shard_topology",
                "fractal_locks",
                "fractal_tenant_migrations",
                "flyway_schema_history",
                "databasechangelog",
                "databasechangeloglock"
        ));
        if (properties.getRebalancer().getExcludeTables() != null) {
            for (String ex : properties.getRebalancer().getExcludeTables()) {
                if (ex != null) exclusions.add(ex.toLowerCase().trim());
            }
        }
        if (validation.getSchemaAuditExcludeTables() != null) {
            for (String ex : validation.getSchemaAuditExcludeTables()) {
                if (ex != null) exclusions.add(ex.toLowerCase().trim());
            }
        }

        Set<String> replicas = new HashSet<>();
        if (properties.getRebalancer().getReplicaTables() != null) {
            for (String r : properties.getRebalancer().getReplicaTables()) {
                if (r != null) replicas.add(r.toLowerCase().trim());
            }
        }

        Set<String> activeShards = properties.getActiveShardNames();
        List<String> violations = new ArrayList<>();

        for (String shardName : activeShards) {
            DataSource ds = shardDataSources.get(shardName);
            if (ds == null) {
                continue;
            }

            List<String> shardTables = discoverTablesOnShard(ds);
            for (String table : shardTables) {
                String normTable = table.toLowerCase().trim();
                if (normTable.equalsIgnoreCase(normRoot)) {
                    continue; // Root table on shard is local slice, permitted!
                }
                if (exclusions.contains(normTable)) {
                    continue;
                }
                if (replicas.contains(normTable)) {
                    continue;
                }

                List<TableForeignKey> path = dependencyResolver != null
                        ? dependencyResolver.findPathToRoot(normTable, normRoot, allFks)
                        : Collections.emptyList();

                if (path.isEmpty()) {
                    violations.add("Physical shard '" + shardName + "' contains table '" + normTable
                            + "' which has no foreign key relationship to root table '" + normRoot
                            + "'. Writes to this table on shards cannot be rebalanced!");
                }
            }
        }

        if (!violations.isEmpty()) {
            StringBuilder sb = new StringBuilder("\n");
            sb.append("====================================================================================================\n");
            sb.append("FRACTAL SHARD SCHEMA AUDIT ").append(mode == FractalProperties.ValidationProperties.EnforcementMode.STRICT ? "FAILURE" : "WARNING").append(":\n");
            sb.append("The following unlinked tables were discovered physically present on worker shard databases:\n");
            for (String v : violations) {
                sb.append("  - ").append(v).append("\n");
            }
            sb.append("To resolve:\n");
            sb.append("  1. Add a foreign key path from the table back to the root table '").append(normRoot).append("'.\n");
            sb.append("  2. If the table is a global read-only lookup table, designate it as a replica table in 'fractal.sharding.rebalancer.replica-tables'.\n");
            sb.append("  3. If the table is not tenant data, exclude it via 'fractal.sharding.validation.schema-audit-exclude-tables'.\n");
            sb.append("  Note: Central coordinator tables existing ONLY on the 'primary' database are permitted and not flagged here.\n");
            sb.append("====================================================================================================");

            if (mode == FractalProperties.ValidationProperties.EnforcementMode.STRICT) {
                throw new IllegalStateException(sb.toString());
            } else {
                log.warn("{}", sb.toString());
            }
        }
    }

    private List<String> discoverTablesOnShard(DataSource ds) {
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        String sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE (table_type = 'BASE TABLE' OR table_type = 'TABLE')
              AND LOWER(table_schema) NOT IN ('information_schema', 'pg_catalog', 'sys', 'performance_schema', 'mysql')
        """;
        List<String> tables = new ArrayList<>();
        try {
            jdbc.query(sql, rs -> {
                String name = rs.getString("table_name");
                if (name != null) {
                    tables.add(name.toLowerCase());
                }
            });
        } catch (Exception e) {
            log.warn("FRACTAL: Error discovering tables on shard during schema audit: {}", e.getMessage());
        }
        return tables;
    }
}
