package io.github.mucchinas.fractal.provisioning;

import org.springframework.security.core.Authentication;

import java.util.Map;

public interface TenantProvisioner {

    record ProvisioningResult(String tenantId, String targetShard, boolean created) {}

    /**
     * Ensures the tenant exists on primary and target shard. If missing, creates the root record
     * and seeds child tables.
     */
    ProvisioningResult ensureProvisioned(String tenantId, Authentication authentication);

    /**
     * Explicit programmatic provisioning with custom column attributes.
     */
    ProvisioningResult provisionIfAbsent(String tenantId, Map<String, Object> customAttributes);

    /**
     * Checks if tenant exists on root table of target shard.
     */
    boolean existsOnShard(String tenantId);

    /**
     * Checks if tenant exists on root table of primary.
     */
    boolean existsOnPrimary(String tenantId);

    /**
     * Resolves target shard for tenant ID.
     */
    String resolveTargetShard(String tenantId);
}
