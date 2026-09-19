package neko.mukynas.fractal.rebalance;

import java.util.Collections;
import java.util.List;

/**
 * Metadata resolved from domain classes annotated with {@link neko.mukynas.fractal.annotation.ShardedEntity},
 * {@link neko.mukynas.fractal.annotation.ShardedKey}, {@link neko.mukynas.fractal.annotation.ShardedStatus},
 * and {@link neko.mukynas.fractal.annotation.ShardedReplica}.
 */
public record EntityMetadataResult(
        String rootTable,
        String rootIdColumn,
        String statusColumn,
        String migratingValue,
        String activeValue,
        List<String> shardedTables,
        List<TableForeignKey> foreignKeys,
        List<String> replicaTables
) {
    public EntityMetadataResult(String rootTable, String rootIdColumn, List<String> shardedTables, List<TableForeignKey> foreignKeys) {
        this(rootTable, rootIdColumn, null, null, null, shardedTables, foreignKeys, Collections.emptyList());
    }

    public EntityMetadataResult(String rootTable, String rootIdColumn, String statusColumn, List<String> shardedTables, List<TableForeignKey> foreignKeys) {
        this(rootTable, rootIdColumn, statusColumn, null, null, shardedTables, foreignKeys, Collections.emptyList());
    }

    public EntityMetadataResult(String rootTable, String rootIdColumn, String statusColumn, String migratingValue, String activeValue, List<String> shardedTables, List<TableForeignKey> foreignKeys) {
        this(rootTable, rootIdColumn, statusColumn, migratingValue, activeValue, shardedTables, foreignKeys, Collections.emptyList());
    }
}
