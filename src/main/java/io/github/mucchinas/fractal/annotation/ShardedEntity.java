package io.github.mucchinas.fractal.annotation;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedEntity {

    String table() default "";

    boolean root() default false;
}
