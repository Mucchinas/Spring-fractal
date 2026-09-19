package neko.mukynas.fractal.rebalance;

import java.util.List;

/**
 * Metadata resolved from domain classes annotated with {@link neko.mukynas.fractal.annotation.ShardedEntity},
 * {@link neko.mukynas.fractal.annotation.ShardedKey}, and {@link neko.mukynas.fractal.annotation.ShardedStatus}.
 */
public record EntityMetadataResult(
        String rootTable,
        String rootIdColumn,
        String statusColumn,
        String migratingValue,
        String activeValue,
        List<String> shardedTables,
        List<TableForeignKey> foreignKeys
) {
    public EntityMetadataResult(String rootTable, String rootIdColumn, List<String> shardedTables, List<TableForeignKey> foreignKeys) {
        this(rootTable, rootIdColumn, null, null, null, shardedTables, foreignKeys);
    }

    public EntityMetadataResult(String rootTable, String rootIdColumn, String statusColumn, List<String> shardedTables, List<TableForeignKey> foreignKeys) {
        this(rootTable, rootIdColumn, statusColumn, null, null, shardedTables, foreignKeys);
    }
}
