package it.neko.mukynas.fractal.exception;

/**
 * Thrown when an operation is attempted on a tenant that is currently
 * undergoing shard migration / rebalancing.
 */
public class TenantMigratingException extends IllegalStateException {

    private final String tenantId;

    public TenantMigratingException(String tenantId) {
        super("Tenant '" + tenantId + "' is currently undergoing shard rebalancing. Access is temporarily suspended.");
        this.tenantId = tenantId;
    }

    public String getTenantId() {
        return tenantId;
    }
}
