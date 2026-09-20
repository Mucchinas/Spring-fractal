package io.github.mucchinas.fractal.annotation;

import java.lang.annotation.*;

@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedBroadcast {

    boolean includePrimary() default true;
}
