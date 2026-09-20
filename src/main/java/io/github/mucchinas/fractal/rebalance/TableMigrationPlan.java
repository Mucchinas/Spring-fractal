package io.github.mucchinas.fractal.rebalance;

public record TableMigrationPlan(
        String tableName,
        String selectSql,
        String deleteSql
) {}
