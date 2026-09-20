package it.neko.mukynas.fractal.annotation;

import java.lang.annotation.*;

@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Sharded {

    /**
     * Espressione SpEL per estrarre la chiave di sharding dai parametri del metodo.
     * Esempio: "#tenantId" oppure "#request.userId"
     */
    String key() default "";
}