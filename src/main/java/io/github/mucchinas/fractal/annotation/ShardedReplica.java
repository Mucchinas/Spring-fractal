package io.github.mucchinas.fractal.annotation;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedReplica {

    String table() default "";
}
