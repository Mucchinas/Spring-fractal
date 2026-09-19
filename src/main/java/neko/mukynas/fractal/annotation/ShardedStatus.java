package neko.mukynas.fractal.annotation;

import java.lang.annotation.*;

/**
 * Marks a field or method on the root {@link ShardedEntity} as the migration synchronization status column.
 * <p>
 * During tenant data rebalancing, Fractal sets this column to the migrating value (default: {@code "MIGRATING"})
 * to lock the tenant from concurrent updates and dirty writes, and restores it to the active value (default: {@code "ACTIVE"})
 * once the rebalance completes.
 * <p>
 * When combined with {@link ShardedEntity} and {@link ShardedKey}, this enables full zero-configuration
 * rebalancing without needing {@code root-table}, {@code root-id-column}, {@code status-column}, or {@code sharded-tables}
 * in {@code application.yml}.
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedStatus {

    /**
     * Physical database column name representing the status.
     * If blank, resolved from JPA @Column(name = "...") or the field/method name in snake_case.
     */
    String column() default "";

    /**
     * Value indicating that the tenant is actively migrating (default: "MIGRATING" or configured YAML value).
     * If blank, defaults to the rebalancer's configured migrating value.
     */
    String migratingValue() default "";

    /**
     * Value indicating that the tenant is active and operational (default: "ACTIVE" or configured YAML value).
     * If blank, defaults to the rebalancer's configured active value.
     */
    String activeValue() default "";
}
