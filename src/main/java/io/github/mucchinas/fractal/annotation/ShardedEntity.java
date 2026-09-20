package io.github.mucchinas.fractal.annotation;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedEntity {

    String table() default "";

    /**
     * @deprecated Use {@link ShardedRoot} to designate the root partition entity instead.
     */
    @Deprecated
    boolean root() default false;
}
