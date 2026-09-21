package io.github.mucchinas.fractal.repository;

import io.github.mucchinas.fractal.annotation.ShardedEntity;
import io.github.mucchinas.fractal.annotation.ShardedKey;
import io.github.mucchinas.fractal.annotation.ShardedReplica;
import io.github.mucchinas.fractal.annotation.ShardedRoot;
import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.rebalance.EntityMetadataResult;
import io.github.mucchinas.fractal.rebalance.EntityTableMetadataResolver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ShardedRepositoryContractTest {

    private AnnotationConfigApplicationContext context;

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    // Entities
    static class UnannotatedEntity {
        private Long id;
    }

    @ShardedRoot(table = "accounts")
    static class AccountRootEntity {
        private String id;
    }

    @ShardedReplica(table = "currencies")
    static class CurrencyReplicaEntity {
        private String code;
    }

    @ShardedEntity(table = "orders")
    static class OrderEntity {
        private Long id;
        @ShardedKey(targetEntity = AccountRootEntity.class)
        private String accountId;
    }

    @ShardedEntity(table = "orphaned_items")
    static class OrphanedItemEntity {
        private Long id;
    }

    // Repository interfaces
    interface UnannotatedEntityRepo extends ShardedRepository<UnannotatedEntity, Long> {}
    interface AccountRootRepo extends ShardedRepository<AccountRootEntity, String> {}
    interface CurrencyReplicaRepo extends ShardedRepository<CurrencyReplicaEntity, String> {}
    interface OrderRepo extends ShardedRepository<OrderEntity, Long> {}
    interface OrphanedItemRepo extends ShardedRepository<OrphanedItemEntity, Long> {}

    @Test
    void shouldThrowWhenShardedRepositoryManagesUnannotatedEntityInStrictMode() {
        context = new AnnotationConfigApplicationContext();
        context.registerBean("unannotatedEntityRepo", UnannotatedEntityRepo.class, () -> mock(UnannotatedEntityRepo.class));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.STRICT);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, null);

        assertThatThrownBy(validator::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("FRACTAL REPOSITORY VALIDATION FAILURE")
                .hasMessageContaining("UnannotatedEntity")
                .hasMessageContaining("is NOT sharded");
    }

    @Test
    void shouldPassWhenShardedRepositoryManagesShardedRootEntity() {
        context = new AnnotationConfigApplicationContext();
        context.registerBean("accountRootRepo", AccountRootRepo.class, () -> mock(AccountRootRepo.class));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.STRICT);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, null);
        validator.validate();
    }

    @Test
    void shouldPassWhenShardedRepositoryManagesShardedReplicaEntity() {
        context = new AnnotationConfigApplicationContext();
        context.registerBean("currencyReplicaRepo", CurrencyReplicaRepo.class, () -> mock(CurrencyReplicaRepo.class));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.STRICT);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, null);
        validator.validate();
    }

    @Test
    void shouldPassWhenShardedEntityHasReachabilityPathToRoot() {
        context = new AnnotationConfigApplicationContext();
        context.registerBean("orderRepo", OrderRepo.class, () -> mock(OrderRepo.class));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.STRICT);

        EntityTableMetadataResolver metadataResolver = mock(EntityTableMetadataResolver.class);
        when(metadataResolver.resolveTableName(OrderEntity.class)).thenReturn("orders");
        EntityMetadataResult metadataResult = new EntityMetadataResult("accounts", "id", List.of("accounts", "orders"), Collections.emptyList());
        when(metadataResolver.resolve()).thenReturn(metadataResult);

        @SuppressWarnings("unchecked")
        ObjectProvider<EntityTableMetadataResolver> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(metadataResolver);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, provider);
        validator.validate();
    }

    @Test
    void shouldThrowWhenShardedEntityHasNoReachabilityPathToRoot() {
        context = new AnnotationConfigApplicationContext();
        context.registerBean("orphanedItemRepo", OrphanedItemRepo.class, () -> mock(OrphanedItemRepo.class));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.STRICT);

        EntityTableMetadataResolver metadataResolver = mock(EntityTableMetadataResolver.class);
        when(metadataResolver.resolveTableName(OrphanedItemEntity.class)).thenReturn("orphaned_items");
        EntityMetadataResult metadataResult = new EntityMetadataResult("accounts", "id", List.of("accounts", "orders"), Collections.emptyList());
        when(metadataResolver.resolve()).thenReturn(metadataResult);

        @SuppressWarnings("unchecked")
        ObjectProvider<EntityTableMetadataResolver> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(metadataResolver);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, provider);

        assertThatThrownBy(validator::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("FRACTAL REPOSITORY VALIDATION FAILURE")
                .hasMessageContaining("OrphanedItemEntity")
                .hasMessageContaining("no reachability path to @ShardedRoot");
    }

    @Test
    void shouldWarnOnlyWhenModeIsWarn() {
        context = new AnnotationConfigApplicationContext();
        context.registerBean("unannotatedEntityRepo", UnannotatedEntityRepo.class, () -> mock(UnannotatedEntityRepo.class));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.WARN);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, null);
        // Warn mode does not throw exception
        validator.validate();
    }

    @Test
    void shouldSkipWhenModeIsDisabled() {
        context = new AnnotationConfigApplicationContext();
        context.registerBean("unannotatedEntityRepo", UnannotatedEntityRepo.class, () -> mock(UnannotatedEntityRepo.class));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.DISABLED);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, null);
        validator.validate();
    }
}
