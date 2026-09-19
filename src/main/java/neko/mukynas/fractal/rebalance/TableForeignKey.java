package neko.mukynas.fractal.rebalance;

/**
 * Representation of a relational foreign key constraint between two tables.
 */
public record TableForeignKey(
        String childTable,
        String childColumn,
        String parentTable,
        String parentColumn
) {}
