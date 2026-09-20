package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.annotation.ShardedReplica;
import jakarta.persistence.Column;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;
import io.github.mucchinas.fractal.annotation.ShardedEntity;
import io.github.mucchinas.fractal.annotation.ShardedKey;
import io.github.mucchinas.fractal.annotation.ShardedStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EntityTableMetadataResolverTest {

    private final EntityTableMetadataResolver resolver = new EntityTableMetadataResolver();

    // 1. Standard hierarchy: Root -> Child -> Grandchild
    @ShardedEntity(root = true)
    static class Organization {
        @ShardedKey
        private String id;
        private String name;
    }

    @ShardedEntity
    static class Project {
        private UUID id;
        @ShardedKey
        private Organization organization;
    }

    @ShardedEntity
    static class Task {
        private UUID id;
        @ShardedKey(targetEntity = Project.class, column = "project_id")
        private UUID projectId;
    }

    @Test
    void shouldResolveThreeTierHierarchy() {
        EntityMetadataResult result = resolver.resolveFromClasses(List.of(Organization.class, Project.class, Task.class));

        assertNotNull(result);
        assertEquals("organization", result.rootTable());
        assertEquals("id", result.rootIdColumn());
        assertEquals(List.of("organization", "project", "task"), result.shardedTables());
        assertEquals(2, result.foreignKeys().size());

        TableForeignKey fk1 = result.foreignKeys().get(0);
        assertEquals("project", fk1.childTable());
        assertEquals("organization", fk1.childColumn());
        assertEquals("organization", fk1.parentTable());
        assertEquals("id", fk1.parentColumn());

        TableForeignKey fk2 = result.foreignKeys().get(1);
        assertEquals("task", fk2.childTable());
        assertEquals("project_id", fk2.childColumn());
        assertEquals("project", fk2.parentTable());
        assertEquals("id", fk2.parentColumn());
    }

    // 2. Explicit table and column names with JPA annotations
    @ShardedEntity(root = true)
    @Table(name = "tenants")
    static class JpaTenant {
        @Id
        @Column(name = "tenant_id")
        private String tenantId;
    }

    @ShardedEntity
    @Table(name = "workspaces")
    static class JpaWorkspace {
        @Id
        private UUID id;

        @ShardedKey
        @JoinColumn(name = "fk_tenant_id")
        private JpaTenant tenant;
    }

    @Test
    void shouldHonorJpaTableAndColumnAnnotations() {
        EntityMetadataResult result = resolver.resolveFromClasses(List.of(JpaTenant.class, JpaWorkspace.class));

        assertNotNull(result);
        assertEquals("tenants", result.rootTable());
        assertEquals("tenant_id", result.rootIdColumn());
        assertEquals(List.of("tenants", "workspaces"), result.shardedTables());
        assertEquals(1, result.foreignKeys().size());

        TableForeignKey fk = result.foreignKeys().get(0);
        assertEquals("workspaces", fk.childTable());
        assertEquals("fk_tenant_id", fk.childColumn());
        assertEquals("tenants", fk.parentTable());
        assertEquals("tenant_id", fk.parentColumn());
    }

    // 3. Error Case: Multiple roots
    @ShardedEntity(root = true)
    static class RootA {
        @ShardedKey
        private String id;
    }

    @ShardedEntity(root = true)
    static class RootB {
        @ShardedKey
        private String id;
    }

    @Test
    void shouldFailWhenMultipleRootsConfigured() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                resolver.resolveFromClasses(List.of(RootA.class, RootB.class))
        );
        assertTrue(ex.getMessage().contains("Multiple root @ShardedEntity entities found"));
    }

    // 4. Error Case: No root
    @ShardedEntity(root = false)
    static class NoRootChild {
        @ShardedKey(targetEntity = Organization.class)
        private String orgId;
    }

    @Test
    void shouldFailWhenNoRootConfigured() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                resolver.resolveFromClasses(List.of(NoRootChild.class))
        );
        assertTrue(ex.getMessage().contains("No root @ShardedEntity entity found"));
    }

    // 5. Error Case: Cycle detected
    @ShardedEntity(root = true)
    static class CycleRoot {
        @ShardedKey
        private String id;
    }

    @ShardedEntity
    static class CycleNodeA {
        @ShardedKey(targetEntity = CycleNodeB.class, column = "b_id")
        private UUID bId;
    }

    @ShardedEntity
    static class CycleNodeB {
        @ShardedKey(targetEntity = CycleNodeA.class, column = "a_id")
        private UUID aId;
    }

    @Test
    void shouldFailWhenCycleDetected() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                resolver.resolveFromClasses(List.of(CycleRoot.class, CycleNodeA.class, CycleNodeB.class))
        );
        assertTrue(ex.getMessage().contains("Cycle detected"));
    }

    // 6. Error Case: Disjoint entity (cannot reach root)
    @ShardedEntity(root = true)
    static class DisjointRoot {
        @ShardedKey
        private String id;
    }

    @ShardedEntity
    static class DisjointParent {
        @ShardedKey(targetEntity = DisjointParent.class, column = "self_id")
        private UUID selfId;
    }

    @Test
    void shouldFailWhenEntityCannotReachRoot() {
        assertThrows(IllegalStateException.class, () ->
                resolver.resolveFromClasses(List.of(DisjointRoot.class, DisjointParent.class))
        );
    }

    // 7. Error Case: Missing @ShardedKey on non-root entity
    @ShardedEntity
    static class MissingKeyChild {
        private UUID id;
    }

    @Test
    void shouldFailWhenDescendantMissingShardedKey() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                resolver.resolveFromClasses(List.of(Organization.class, MissingKeyChild.class))
        );
        assertTrue(ex.getMessage().contains("has no @ShardedKey annotation"));
    }

    // 8. Error Case: Scalar field missing targetEntity
    @ShardedEntity
    static class MissingTargetEntityChild {
        @ShardedKey
        private UUID parentId;
    }

    @Test
    void shouldFailWhenScalarFieldOmitsTargetEntity() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                resolver.resolveFromClasses(List.of(Organization.class, MissingTargetEntityChild.class))
        );
        assertTrue(ex.getMessage().contains("Cannot determine target parent entity"));
    }

    // 9. Root entity with @ShardedStatus
    @ShardedEntity(root = true)
    @Table(name = "accounts")
    static class AccountWithStatus {
        @Id
        private String id;

        @ShardedStatus
        @Column(name = "sync_status")
        private String syncStatus;
    }

    @Test
    void shouldResolveShardedStatusFromRootEntity() {
        EntityMetadataResult result = resolver.resolveFromClasses(List.of(AccountWithStatus.class));

        assertNotNull(result);
        assertEquals("accounts", result.rootTable());
        assertEquals("id", result.rootIdColumn());
        assertEquals("sync_status", result.statusColumn());
        assertNull(result.migratingValue());
        assertNull(result.activeValue());
    }

    // 10. Root entity with explicit @ShardedStatus values
    @ShardedEntity(root = true)
    static class AccountWithCustomStatusValues {
        @ShardedKey
        private String id;

        @ShardedStatus(column = "migration_phase", migratingValue = "MOVING", activeValue = "READY")
        private String status;
    }

    @Test
    void shouldResolveCustomShardedStatusValues() {
        EntityMetadataResult result = resolver.resolveFromClasses(List.of(AccountWithCustomStatusValues.class));

        assertNotNull(result);
        assertEquals("migration_phase", result.statusColumn());
        assertEquals("MOVING", result.migratingValue());
        assertEquals("READY", result.activeValue());
    }

    // 11. Error: multiple @ShardedStatus on root entity
    @ShardedEntity(root = true)
    static class DuplicateStatusEntity {
        @ShardedKey
        private String id;

        @ShardedStatus
        private String status1;

        @ShardedStatus
        private String status2;
    }

    @Test
    void shouldFailWhenMultipleShardedStatusOnRoot() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                resolver.resolveFromClasses(List.of(DuplicateStatusEntity.class))
        );
        assertTrue(ex.getMessage().contains("Multiple @ShardedStatus annotations found"));
    }

    // 12. Error: @ShardedStatus on non-root entity
    @ShardedEntity
    static class NonRootWithStatus {
        @ShardedKey(targetEntity = Organization.class, column = "org_id")
        private String orgId;

        @ShardedStatus
        private String status;
    }

    @Test
    void shouldFailWhenShardedStatusOnNonRoot() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                resolver.resolveFromClasses(List.of(Organization.class, NonRootWithStatus.class))
        );
        assertTrue(ex.getMessage().contains("is only permitted on the root entity"));
    }

    // 13. Replicated tables (@ShardedReplica)
    @ShardedReplica(table = "currencies")
    static class Currency {
        private String code;
        private Double rate;
    }

    @ShardedReplica
    @Table(name = "system_roles")
    static class Role {
        private String roleName;
    }

    @Test
    void shouldResolveShardedReplicaTables() {
        EntityMetadataResult result = resolver.resolveFromClasses(
                List.of(Organization.class, Project.class, Currency.class, Role.class)
        );

        assertNotNull(result);
        assertEquals(List.of("currencies", "system_roles"), result.replicaTables());
        assertEquals(List.of("organization", "project"), result.shardedTables());
    }

    @Test
    void shouldResolveOnlyReplicaTablesWhenNoShardedEntityPresent() {
        EntityMetadataResult result = resolver.resolveFromClasses(
                List.of(Currency.class, Role.class)
        );

        assertNotNull(result);
        assertNull(result.rootTable());
        assertTrue(result.shardedTables().isEmpty());
        assertEquals(List.of("currencies", "system_roles"), result.replicaTables());
    }
}
