package io.github.mucchinas.fractal.rebalance;

/**
 * Metadata record describing how to select and delete rows for a specific table
 * during shard migration, taking into account foreign key relationships.
 */
public record TableMigrationPlan(
        String tableName,
        String selectSql,
        String deleteSql
) {}
