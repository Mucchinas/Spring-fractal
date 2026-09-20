package it.neko.mukynas.fractal.annotation;

import java.lang.annotation.*;

/**
 * Marks a class as a sharded entity in the database cluster.
 * Enables zero-config table auto-discovery, topological rebalance planning,
 * and foreign key dependency resolution.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedEntity {

    /**
     * Physical database table name.
     * If blank, resolved from JPA @Table(name = "...") or the entity class name in snake_case.
     */
    String table() default "";

    /**
     * Declares whether this entity is the root partition anchor of the sharded cluster (e.g. Tenant, Organization).
     * Exactly one entity in the application must be declared with root = true.
     */
    boolean root() default false;
}
