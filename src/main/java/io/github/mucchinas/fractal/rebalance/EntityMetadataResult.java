package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.annotation.ShardedEntity;
import io.github.mucchinas.fractal.annotation.ShardedKey;
import io.github.mucchinas.fractal.annotation.ShardedReplica;
import io.github.mucchinas.fractal.annotation.ShardedStatus;

import java.util.Collections;
import java.util.List;

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
