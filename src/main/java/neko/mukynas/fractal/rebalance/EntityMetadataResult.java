package neko.mukynas.fractal.rebalance;

import java.util.List;

/**
 * Metadata resolved from domain classes annotated with {@link neko.mukynas.fractal.annotation.ShardedEntity}
 * and {@link neko.mukynas.fractal.annotation.ShardedKey}.
 */
public record EntityMetadataResult(
        String rootTable,
        String rootIdColumn,
        List<String> shardedTables,
        List<TableForeignKey> foreignKeys
) {}
