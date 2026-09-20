package io.github.mucchinas.fractal.annotation;

import java.lang.annotation.*;

/**
 * Designates the root partition entity in a sharded domain model.
 *
 * <p>The field or method annotated with {@link ShardedKey} (or JPA {@code @Id})
 * on this entity acts as the primary cluster sharding key (root table and partition column).
 * All descendant {@link ShardedEntity} classes must establish a chain of {@link ShardedKey}
 * hops leading back to this root entity.</p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedRoot {

    /**
     * Optional explicit database table name for this root entity.
     * If omitted, falls back to JPA {@code @Table(name = "...")} or snake_case class name.
     */
    String table() default "";
}
