package it.neko.mukynas.fractal.annotation;

import java.lang.annotation.*;

/**
 * Executes a write operation across the primary coordinator database and all configured
 * physical shards in the cluster.
 *
 * <p>Typically placed on service methods that create, update, or delete reference data
 * in {@link ShardedReplica} tables (such as updating currency conversion rates or creating
 * new system roles), ensuring changes are reflected immediately across all physical shard databases.</p>
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedBroadcast {

    /**
     * Whether the invocation should also execute against the primary database.
     * Default is {@code true}.
     */
    boolean includePrimary() default true;
}
