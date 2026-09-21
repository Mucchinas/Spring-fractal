package io.github.mucchinas.fractal.provisioning;

import org.springframework.security.core.Authentication;

import java.util.Map;

@FunctionalInterface
public interface RootEntityCustomizer {

    /**
     * Programmatically customize or supplement root entity table columns before insert into
     * primary database and target shard.
     *
     * @param tenantId the newly provisioned tenant/user ID
     * @param columns mutable map of column names to values
     * @param authentication the current Authentication context (may be null)
     */
    void customize(String tenantId, Map<String, Object> columns, Authentication authentication);
}
