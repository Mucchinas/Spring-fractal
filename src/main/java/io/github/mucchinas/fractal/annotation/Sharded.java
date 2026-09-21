package io.github.mucchinas.fractal.annotation;

import java.lang.annotation.*;

@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Sharded {

    String key() default "";

    /**
     * If true, enables Just-In-Time (JIT) runtime provisioning for this method.
     * When the sharding key is not found as the primary key of the @ShardedRoot entity
     * in the database root table, Fractal automatically creates the row on primary and target shard.
     *
     * Defaults to false, ensuring zero provisioning overhead on standard methods.
     */
    boolean provision() default false;
}