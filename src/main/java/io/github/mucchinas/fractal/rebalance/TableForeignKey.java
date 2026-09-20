package io.github.mucchinas.fractal.rebalance;

public record TableForeignKey(
        String childTable,
        String childColumn,
        String parentTable,
        String parentColumn
) {}
