package io.github.mucchinas.fractal.provisioning;

import java.util.Map;

@FunctionalInterface
public interface TenantInitializer {

    /**
     * Invoked immediately after the root record is created on the target shard,
     * while the current thread's ShardContextHolder is bound to the target shard.
     *
     * @param tenantId the newly provisioned tenant/user ID
     * @param targetShard the routed target shard name
     * @param rootAttributes read-only map of root table column values
     */
    void initializeTenant(String tenantId, String targetShard, Map<String, Object> rootAttributes);
}
