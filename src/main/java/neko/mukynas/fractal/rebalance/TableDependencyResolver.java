package neko.mukynas.fractal.rebalance;

import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.*;

public class TableDependencyResolver {

    private final JdbcTemplate primaryJdbcTemplate;
    private List<TableForeignKey> explicitForeignKeys;

    public TableDependencyResolver(DataSource primaryDataSource) {
        this(primaryDataSource, null);
    }

    public TableDependencyResolver(DataSource primaryDataSource, List<TableForeignKey> explicitForeignKeys) {
        this.primaryJdbcTemplate = primaryDataSource != null ? new JdbcTemplate(primaryDataSource) : null;
        this.explicitForeignKeys = explicitForeignKeys;
    }

    public void setExplicitForeignKeys(List<TableForeignKey> explicitForeignKeys) {
        this.explicitForeignKeys = explicitForeignKeys;
    }

    /**
     * Loads all foreign key constraints from explicit definitions or system metadata catalog.
     * Uses ANSI standard information_schema tables supported by PostgreSQL, H2, and other standard SQL engines.
     */
    public List<TableForeignKey> loadForeignKeys() {
        if (explicitForeignKeys != null && !explicitForeignKeys.isEmpty()) {
            return explicitForeignKeys;
        }
        if (primaryJdbcTemplate == null) {
            return Collections.emptyList();
        }
        String sql = """
            SELECT
                kcu.table_name AS child_table,
                kcu.column_name AS child_column,
                pk_kcu.table_name AS parent_table,
                pk_kcu.column_name AS parent_column
            FROM
                information_schema.referential_constraints AS rc
                JOIN information_schema.key_column_usage AS kcu
                  ON rc.constraint_name = kcu.constraint_name
                  AND rc.constraint_schema = kcu.constraint_schema
                JOIN information_schema.key_column_usage AS pk_kcu
                  ON rc.unique_constraint_name = pk_kcu.constraint_name
                  AND rc.unique_constraint_schema = pk_kcu.constraint_schema
                  AND kcu.ordinal_position = pk_kcu.ordinal_position
        """;

        List<TableForeignKey> fks = new ArrayList<>();
        primaryJdbcTemplate.query(sql, rs -> {
            String child = rs.getString("child_table");
            String childCol = rs.getString("child_column");
            String parent = rs.getString("parent_table");
            String parentCol = rs.getString("parent_column");
            if (child != null && parent != null) {
                fks.add(new TableForeignKey(child.toLowerCase(), childCol.toLowerCase(), parent.toLowerCase(), parentCol.toLowerCase()));
            }
        });
        return fks;
    }

    /**
     * Discovers all user tables in the database catalog (excluding system schemas and Fractal metadata tables),
     * filtering out tables in excludeTables.
     */
    public List<String> discoverAllDatabaseTables(List<String> excludeTables) {
        if (primaryJdbcTemplate == null) {
            return Collections.emptyList();
        }

        Set<String> exclusions = new HashSet<>(Arrays.asList(
                "fractal_shard_topology",
                "fractal_locks",
                "fractal_tenant_migrations"
        ));
        if (excludeTables != null) {
            for (String ex : excludeTables) {
                if (ex != null) exclusions.add(ex.toLowerCase());
            }
        }

        String sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE (table_type = 'BASE TABLE' OR table_type = 'TABLE')
              AND LOWER(table_schema) NOT IN ('information_schema', 'pg_catalog', 'sys', 'performance_schema', 'mysql')
        """;

        List<String> tables = new ArrayList<>();
        try {
            primaryJdbcTemplate.query(sql, rs -> {
                String name = rs.getString("table_name");
                if (name != null) {
                    String lower = name.toLowerCase();
                    if (!exclusions.contains(lower) && !tables.contains(lower)) {
                        tables.add(lower);
                    }
                }
            });
        } catch (Exception e) {
            System.err.println("FRACTAL: Error discovering all tables from information_schema: " + e.getMessage());
        }
        return tables;
    }

    /**
     * Auto-discovers all sharded tables starting from rootTable by tracing foreign keys.
     * If explicitTables is provided and non-empty, it is used instead of auto-discovery.
     */
    public List<String> discoverShardedTables(String rootTable, List<String> explicitTables, List<String> excludeTables) {
        return discoverShardedTables(rootTable, explicitTables, excludeTables, false);
    }

    /**
     * Discovers sharded tables. If shardAll is true, includes all database tables except exclusions.
     */
    public List<String> discoverShardedTables(String rootTable, List<String> explicitTables, List<String> excludeTables, boolean shardAll) {
        if (shardAll) {
            List<String> allTables = discoverAllDatabaseTables(excludeTables);
            if (rootTable != null && !rootTable.isBlank()) {
                String normRoot = rootTable.toLowerCase();
                if (allTables.contains(normRoot)) {
                    allTables.remove(normRoot);
                    allTables.add(0, normRoot);
                } else {
                    allTables.add(0, normRoot);
                }
            }
            return allTables;
        }

        if (rootTable == null || rootTable.isBlank()) {
            return explicitTables != null ? explicitTables : Collections.emptyList();
        }

        Set<String> exclusions = new HashSet<>();
        if (excludeTables != null) {
            for (String ex : excludeTables) {
                if (ex != null) exclusions.add(ex.toLowerCase());
            }
        }

        if (explicitTables != null && !explicitTables.isEmpty()) {
            List<String> filtered = new ArrayList<>();
            for (String table : explicitTables) {
                if (table != null && !exclusions.contains(table.toLowerCase())) {
                    filtered.add(table.toLowerCase());
                }
            }
            return filtered;
        }

        List<TableForeignKey> allFks = loadForeignKeys();
        Set<String> discovered = new LinkedHashSet<>();
        Queue<String> queue = new LinkedList<>();

        String normalizedRoot = rootTable.toLowerCase();
        discovered.add(normalizedRoot);
        queue.offer(normalizedRoot);

        while (!queue.isEmpty()) {
            String currentParent = queue.poll();
            for (TableForeignKey fk : allFks) {
                if (fk.parentTable().equalsIgnoreCase(currentParent)) {
                    String child = fk.childTable().toLowerCase();
                    if (!exclusions.contains(child) && discovered.add(child)) {
                        queue.offer(child);
                    }
                }
            }
        }

        return new ArrayList<>(discovered);
    }

    /**
     * Resolves topological sort order for insertion (parents before children).
     */
    public List<String> resolveInsertOrder(List<String> tables) {
        if (tables == null || tables.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> normalizedTables = tables.stream().filter(Objects::nonNull).map(String::toLowerCase).distinct().toList();
        List<TableForeignKey> allFks = loadForeignKeys();

        Map<String, List<String>> graph = new HashMap<>();
        Map<String, Integer> inDegree = new HashMap<>();

        for (String table : normalizedTables) {
            graph.put(table, new ArrayList<>());
            inDegree.put(table, 0);
        }

        for (TableForeignKey fk : allFks) {
            String child = fk.childTable();
            String parent = fk.parentTable();
            if (graph.containsKey(child) && graph.containsKey(parent) && !child.equals(parent)) {
                if (!graph.get(parent).contains(child)) {
                    graph.get(parent).add(child);
                    inDegree.put(child, inDegree.get(child) + 1);
                }
            }
        }

        Queue<String> queue = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.offer(entry.getKey());
            }
        }

        List<String> insertOrder = new ArrayList<>();
        while (!queue.isEmpty()) {
            String current = queue.poll();
            insertOrder.add(current);

            for (String child : graph.get(current)) {
                inDegree.put(child, inDegree.get(child) - 1);
                if (inDegree.get(child) == 0) {
                    queue.offer(child);
                }
            }
        }

        if (insertOrder.size() != normalizedTables.size()) {
            throw new IllegalStateException("Rilevato ciclo nelle Foreign Key o tabelle mancanti. " +
                    "Il Topological Sort è fallito. Controlla lo schema del database.");
        }

        return insertOrder;
    }

    /**
     * Delete order is the reverse of insert order (children before parents).
     */
    public List<String> resolveDeleteOrder(List<String> tables) {
        List<String> insertOrder = resolveInsertOrder(tables);
        List<String> deleteOrder = new ArrayList<>(insertOrder);
        Collections.reverse(deleteOrder);
        return deleteOrder;
    }

    /**
     * Builds migration plans (SELECT and DELETE queries with foreign key hopping)
     * for all sharded tables in topological insert order.
     */
    public List<TableMigrationPlan> resolveMigrationPlans(String rootTable,
                                                         String rootIdColumn,
                                                         List<String> explicitTables,
                                                         List<String> excludeTables) {
        return resolveMigrationPlans(rootTable, rootIdColumn, explicitTables, excludeTables, false);
    }

    /**
     * Builds migration plans (SELECT and DELETE queries with foreign key hopping)
     * for all sharded tables in topological insert order, supporting shardAll flag.
     */
    public List<TableMigrationPlan> resolveMigrationPlans(String rootTable,
                                                         String rootIdColumn,
                                                         List<String> explicitTables,
                                                         List<String> excludeTables,
                                                         boolean shardAll) {
        if (rootTable == null || rootTable.isBlank()) {
            return Collections.emptyList();
        }

        String normRoot = rootTable.toLowerCase();
        String normRootId = rootIdColumn != null ? rootIdColumn.toLowerCase() : "id";

        List<String> tables = discoverShardedTables(normRoot, explicitTables, excludeTables, shardAll);
        List<String> orderedTables = resolveInsertOrder(tables);
        List<TableForeignKey> allFks = loadForeignKeys();

        List<TableMigrationPlan> plans = new ArrayList<>();
        for (String table : orderedTables) {
            plans.add(buildTablePlan(table, normRoot, normRootId, allFks));
        }

        return plans;
    }

    private TableMigrationPlan buildTablePlan(String table, String rootTable, String rootIdColumn, List<TableForeignKey> allFks) {
        if (table.equalsIgnoreCase(rootTable)) {
            String selectSql = String.format("SELECT * FROM %s WHERE %s = :userId", rootTable, rootIdColumn);
            String deleteSql = String.format("DELETE FROM %s WHERE %s = :userId", rootTable, rootIdColumn);
            return new TableMigrationPlan(rootTable, selectSql, deleteSql);
        }

        // Find shortest path from table to rootTable
        List<TableForeignKey> path = findPathToRoot(table, rootTable, allFks);
        if (path.isEmpty()) {
            // Fallback if no FK path exists: direct filter on rootIdColumn
            String selectSql = String.format("SELECT * FROM %s WHERE %s = :userId", table, rootIdColumn);
            String deleteSql = String.format("DELETE FROM %s WHERE %s = :userId", table, rootIdColumn);
            return new TableMigrationPlan(table, selectSql, deleteSql);
        }

        // Build SELECT query with JOINs
        String selectSql;
        if (path.size() == 1) {
            selectSql = String.format("SELECT * FROM %s WHERE %s = :userId", table, path.get(0).childColumn());
        } else {
            StringBuilder select = new StringBuilder("SELECT ").append(table).append(".* FROM ").append(table);
            for (TableForeignKey fk : path) {
                select.append(" JOIN ").append(fk.parentTable())
                      .append(" ON ").append(fk.childTable()).append(".").append(fk.childColumn())
                      .append(" = ").append(fk.parentTable()).append(".").append(fk.parentColumn());
            }
            select.append(" WHERE ").append(rootTable).append(".").append(rootIdColumn).append(" = :userId");
            selectSql = select.toString();
        }

        // Build DELETE query with subqueries
        String deleteSql;
        if (path.size() == 1) {
            deleteSql = String.format("DELETE FROM %s WHERE %s = :userId", table, path.get(0).childColumn());
        } else {
            StringBuilder delete = new StringBuilder("DELETE FROM ").append(table).append(" WHERE ").append(path.get(0).childColumn());
            for (int i = 0; i < path.size() - 1; i++) {
                TableForeignKey cur = path.get(i);
                TableForeignKey next = path.get(i + 1);
                delete.append(" IN (SELECT ").append(cur.parentColumn())
                      .append(" FROM ").append(cur.parentTable())
                      .append(" WHERE ").append(next.childColumn());
            }
            delete.append(" = :userId");
            for (int i = 0; i < path.size() - 1; i++) {
                delete.append(")");
            }
            deleteSql = delete.toString();
        }

        return new TableMigrationPlan(table, selectSql, deleteSql);
    }

    private List<TableForeignKey> findPathToRoot(String startTable, String rootTable, List<TableForeignKey> allFks) {
        Queue<List<TableForeignKey>> queue = new LinkedList<>();
        for (TableForeignKey fk : allFks) {
            if (fk.childTable().equalsIgnoreCase(startTable)) {
                List<TableForeignKey> initial = new ArrayList<>();
                initial.add(fk);
                if (fk.parentTable().equalsIgnoreCase(rootTable)) {
                    return initial;
                }
                queue.offer(initial);
            }
        }

        Set<String> visited = new HashSet<>();
        visited.add(startTable.toLowerCase());

        while (!queue.isEmpty()) {
            List<TableForeignKey> currentPath = queue.poll();
            TableForeignKey lastEdge = currentPath.get(currentPath.size() - 1);
            String currentParent = lastEdge.parentTable();

            if (!visited.add(currentParent)) {
                continue;
            }

            for (TableForeignKey nextFk : allFks) {
                if (nextFk.childTable().equalsIgnoreCase(currentParent)) {
                    List<TableForeignKey> newPath = new ArrayList<>(currentPath);
                    newPath.add(nextFk);
                    if (nextFk.parentTable().equalsIgnoreCase(rootTable)) {
                        return newPath;
                    }
                    queue.offer(newPath);
                }
            }
        }

        return Collections.emptyList();
    }
}