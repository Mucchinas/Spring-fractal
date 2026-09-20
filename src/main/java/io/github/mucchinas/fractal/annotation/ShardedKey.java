package io.github.mucchinas.fractal.annotation;

import java.lang.annotation.*;

@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedKey {

    String column() default "";

    Class<?> targetEntity() default Void.class;

    String referencedColumn() default "";
}
