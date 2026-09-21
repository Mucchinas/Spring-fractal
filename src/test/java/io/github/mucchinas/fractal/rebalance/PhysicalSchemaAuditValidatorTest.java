package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.config.FractalProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PhysicalSchemaAuditValidatorTest {

    private DriverManagerDataSource shardDataSource;
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        shardDataSource = new DriverManagerDataSource();
        shardDataSource.setDriverClassName("org.h2.Driver");
        shardDataSource.setUrl("jdbc:h2:mem:shard_schema_audit_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shardDataSource.setUsername("sa");
        shardDataSource.setPassword("");
        jdbcTemplate = new JdbcTemplate(shardDataSource);
        jdbcTemplate.execute("DROP ALL OBJECTS");
    }

    @AfterEach
    void tearDown() {
        jdbcTemplate.execute("DROP ALL OBJECTS");
    }

    @Test
    void shouldThrowWhenShardHasUnlinkedTableInStrictMode() {
        jdbcTemplate.execute("CREATE TABLE accounts (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255))");
        jdbcTemplate.execute("CREATE TABLE orders (id VARCHAR(255) PRIMARY KEY, account_id VARCHAR(255), FOREIGN KEY (account_id) REFERENCES accounts(id))");
        jdbcTemplate.execute("CREATE TABLE orphaned_data (id VARCHAR(255) PRIMARY KEY, payload VARCHAR(255))");

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setRootTable("accounts");
        properties.setShards(new java.util.HashMap<>());
        properties.getShards().put("shard-1", new FractalProperties.DataSourceProperties());
        properties.getValidation().setSchemaAuditAction(FractalProperties.ValidationProperties.EnforcementMode.STRICT);

        TableDependencyResolver dependencyResolver = mock(TableDependencyResolver.class);
        List<TableForeignKey> fks = List.of(new TableForeignKey("orders", "account_id", "accounts", "id"));
        when(dependencyResolver.loadForeignKeys()).thenReturn(fks);
        when(dependencyResolver.findPathToRoot(eq("orders"), eq("accounts"), any())).thenReturn(fks);
        when(dependencyResolver.findPathToRoot(eq("orphaned_data"), eq("accounts"), any())).thenReturn(Collections.emptyList());

        PhysicalSchemaAuditValidator validator = new PhysicalSchemaAuditValidator(
                Map.of("shard-1", shardDataSource),
                properties,
                dependencyResolver
        );

        assertThatThrownBy(validator::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("FRACTAL SHARD SCHEMA AUDIT FAILURE")
                .hasMessageContaining("orphaned_data")
                .hasMessageContaining("shard-1");
    }

    @Test
    void shouldNotThrowWhenModeIsWarn() {
        jdbcTemplate.execute("CREATE TABLE accounts (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255))");
        jdbcTemplate.execute("CREATE TABLE orphaned_data (id VARCHAR(255) PRIMARY KEY, payload VARCHAR(255))");

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setRootTable("accounts");
        properties.setShards(new java.util.HashMap<>());
        properties.getShards().put("shard-1", new FractalProperties.DataSourceProperties());
        properties.getValidation().setSchemaAuditAction(FractalProperties.ValidationProperties.EnforcementMode.WARN);

        TableDependencyResolver dependencyResolver = mock(TableDependencyResolver.class);
        when(dependencyResolver.loadForeignKeys()).thenReturn(Collections.emptyList());
        when(dependencyResolver.findPathToRoot(eq("orphaned_data"), eq("accounts"), any())).thenReturn(Collections.emptyList());

        PhysicalSchemaAuditValidator validator = new PhysicalSchemaAuditValidator(
                Map.of("shard-1", shardDataSource),
                properties,
                dependencyResolver
        );

        // Does not throw in WARN mode
        validator.validate();
    }

    @Test
    void shouldSkipValidationWhenModeIsDisabled() {
        jdbcTemplate.execute("CREATE TABLE accounts (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255))");
        jdbcTemplate.execute("CREATE TABLE orphaned_data (id VARCHAR(255) PRIMARY KEY, payload VARCHAR(255))");

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setRootTable("accounts");
        properties.setShards(new java.util.HashMap<>());
        properties.getShards().put("shard-1", new FractalProperties.DataSourceProperties());
        properties.getValidation().setSchemaAuditAction(FractalProperties.ValidationProperties.EnforcementMode.DISABLED);

        TableDependencyResolver dependencyResolver = mock(TableDependencyResolver.class);

        PhysicalSchemaAuditValidator validator = new PhysicalSchemaAuditValidator(
                Map.of("shard-1", shardDataSource),
                properties,
                dependencyResolver
        );

        validator.validate();
    }

    @Test
    void shouldIgnoreExcludedTables() {
        jdbcTemplate.execute("CREATE TABLE accounts (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255))");
        jdbcTemplate.execute("CREATE TABLE audit_logs (id VARCHAR(255) PRIMARY KEY, action VARCHAR(255))");

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setRootTable("accounts");
        properties.setShards(new java.util.HashMap<>());
        properties.getShards().put("shard-1", new FractalProperties.DataSourceProperties());
        properties.getValidation().setSchemaAuditAction(FractalProperties.ValidationProperties.EnforcementMode.STRICT);
        properties.getValidation().setSchemaAuditExcludeTables(List.of("audit_logs"));

        TableDependencyResolver dependencyResolver = mock(TableDependencyResolver.class);
        when(dependencyResolver.loadForeignKeys()).thenReturn(Collections.emptyList());

        PhysicalSchemaAuditValidator validator = new PhysicalSchemaAuditValidator(
                Map.of("shard-1", shardDataSource),
                properties,
                dependencyResolver
        );

        // audit_logs is excluded, so validate passes without error
        validator.validate();
    }

    @Test
    void shouldIgnoreReplicaTables() {
        jdbcTemplate.execute("CREATE TABLE accounts (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255))");
        jdbcTemplate.execute("CREATE TABLE countries (code VARCHAR(10) PRIMARY KEY, name VARCHAR(255))");

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setRootTable("accounts");
        properties.setShards(new java.util.HashMap<>());
        properties.getShards().put("shard-1", new FractalProperties.DataSourceProperties());
        properties.getValidation().setSchemaAuditAction(FractalProperties.ValidationProperties.EnforcementMode.STRICT);
        properties.getRebalancer().setReplicaTables(List.of("countries"));

        TableDependencyResolver dependencyResolver = mock(TableDependencyResolver.class);
        when(dependencyResolver.loadForeignKeys()).thenReturn(Collections.emptyList());

        PhysicalSchemaAuditValidator validator = new PhysicalSchemaAuditValidator(
                Map.of("shard-1", shardDataSource),
                properties,
                dependencyResolver
        );

        // countries is a designated replica table, so validate passes
        validator.validate();
    }

    @Test
    void shouldPermitTablesOnlyOnPrimaryCoordinator() {
        // Shard only has accounts and linked orders
        jdbcTemplate.execute("CREATE TABLE accounts (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255))");
        jdbcTemplate.execute("CREATE TABLE orders (id VARCHAR(255) PRIMARY KEY, account_id VARCHAR(255), FOREIGN KEY (account_id) REFERENCES accounts(id))");

        // Primary database contains central coordinator tables like billing_plans, system_config
        DriverManagerDataSource primaryDs = new DriverManagerDataSource();
        primaryDs.setDriverClassName("org.h2.Driver");
        primaryDs.setUrl("jdbc:h2:mem:primary_audit_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        primaryDs.setUsername("sa");
        primaryDs.setPassword("");
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        primaryJdbc.execute("DROP ALL OBJECTS");
        primaryJdbc.execute("CREATE TABLE global_billing_plans (plan_id VARCHAR(50) PRIMARY KEY, price NUMERIC)");
        primaryJdbc.execute("CREATE TABLE system_config (k VARCHAR(50) PRIMARY KEY, v VARCHAR(50))");

        FractalProperties properties = new FractalProperties();
        properties.getRebalancer().setRootTable("accounts");
        properties.setShards(new java.util.HashMap<>());
        properties.getShards().put("shard-1", new FractalProperties.DataSourceProperties());
        properties.getValidation().setSchemaAuditAction(FractalProperties.ValidationProperties.EnforcementMode.STRICT);

        TableDependencyResolver dependencyResolver = mock(TableDependencyResolver.class);
        List<TableForeignKey> fks = List.of(new TableForeignKey("orders", "account_id", "accounts", "id"));
        when(dependencyResolver.loadForeignKeys()).thenReturn(fks);
        when(dependencyResolver.findPathToRoot(eq("orders"), eq("accounts"), any())).thenReturn(fks);

        PhysicalSchemaAuditValidator validator = new PhysicalSchemaAuditValidator(
                Map.of("shard-1", shardDataSource), // Only shard DataSources are checked
                properties,
                dependencyResolver
        );

        // Validates with 0 errors because global_billing_plans is on primary, NOT on the shard
        validator.validate();
        primaryJdbc.execute("DROP ALL OBJECTS");
    }
}
