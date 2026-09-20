package io.github.mucchinas.fractal.annotation;

import java.lang.annotation.*;

@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedStatus {

    String column() default "";

    String migratingValue() default "";

    String activeValue() default "";
}
