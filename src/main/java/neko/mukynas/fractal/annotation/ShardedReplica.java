package neko.mukynas.fractal.annotation;

import java.lang.annotation.*;

/**
 * Designates a database entity/table as a Replicated (Broadcast) Table.
 *
 * <p>Unlike sharded entities which are partitioned horizontally by tenant key,
 * replicated entities represent shared reference or lookup data (such as currencies,
 * countries, product categories, or system roles). 100% of their records are synchronized
 * across all physical shards, allowing local relational SQL queries and JOINs on any shard
 * without cross-database network hops.</p>
 *
 * <p>Replicated tables are never deleted or pruned during tenant rebalancing.</p>
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedReplica {

    /**
     * Explicit database table name. If blank, the table name is inferred from
     * {@code @Table(name = "...")} or the entity class name.
     */
    String table() default "";
}
