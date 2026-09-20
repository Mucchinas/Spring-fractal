package it.neko.mukynas.fractal.rebalance;

import it.neko.mukynas.fractal.annotation.ShardedEntity;
import it.neko.mukynas.fractal.annotation.ShardedKey;
import it.neko.mukynas.fractal.annotation.ShardedReplica;
import it.neko.mukynas.fractal.annotation.ShardedStatus;

import java.util.Collections;
import java.util.List;

/**
 * Metadata resolved from domain classes annotated with {@link ShardedEntity},
 * {@link ShardedKey}, {@link ShardedStatus},
 * and {@link ShardedReplica}.
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
