package io.github.mucchinas.fractal.repository;

import io.github.mucchinas.fractal.annotation.Sharded;
import io.github.mucchinas.fractal.annotation.ShardedRoot;
import io.github.mucchinas.fractal.config.FractalProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.Repository;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class ShardedServiceInjectionValidationTest {

    private AnnotationConfigApplicationContext context;

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    // Entities
    @ShardedRoot(table = "accounts")
    static class AccountEntity {
        private String id;
    }

    static class BillingPlanEntity {
        private String planId;
    }

    // Repositories
    interface ValidAccountShardedRepo extends ShardedRepository<AccountEntity, String> {}
    interface CentralBillingPlanRepo extends JpaRepository<BillingPlanEntity, String> {}

    // Service 1: @Sharded at class level injecting non-sharded repository
    @Sharded(key = "#tenantId")
    static class InvalidClassLevelShardedService {
        private final CentralBillingPlanRepo billingRepo;

        public InvalidClassLevelShardedService(CentralBillingPlanRepo billingRepo) {
            this.billingRepo = billingRepo;
        }
    }

    // Service 2: @Sharded at method level injecting non-sharded repository
    static class InvalidMethodLevelShardedService {
        private final CentralBillingPlanRepo billingRepo;

        public InvalidMethodLevelShardedService(CentralBillingPlanRepo billingRepo) {
            this.billingRepo = billingRepo;
        }

        @Sharded(key = "#tenantId")
        public void doWork(String tenantId) {
        }
    }

    // Service 3: Valid @Sharded service injecting only ShardedRepository
    @Sharded(key = "#tenantId")
    static class ValidShardedService {
        private final ValidAccountShardedRepo accountRepo;

        public ValidShardedService(ValidAccountShardedRepo accountRepo) {
            this.accountRepo = accountRepo;
        }
    }

    // Service 4: Coordinator-only service (no @Sharded annotation) injecting standard repository
    static class CoordinatorService {
        private final CentralBillingPlanRepo billingRepo;

        public CoordinatorService(CentralBillingPlanRepo billingRepo) {
            this.billingRepo = billingRepo;
        }
    }

    @Test
    void shouldThrowWhenClassLevelShardedServiceInjectsNonShardedRepositoryInStrictMode() {
        context = new AnnotationConfigApplicationContext();
        CentralBillingPlanRepo mockBillingRepo = mock(CentralBillingPlanRepo.class);
        context.registerBean("billingRepo", CentralBillingPlanRepo.class, () -> mockBillingRepo);
        context.registerBean("invalidService", InvalidClassLevelShardedService.class, () -> new InvalidClassLevelShardedService(mockBillingRepo));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.STRICT);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, null);

        assertThatThrownBy(validator::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("FRACTAL REPOSITORY VALIDATION FAILURE")
                .hasMessageContaining("InvalidClassLevelShardedService")
                .hasMessageContaining("CentralBillingPlanRepo")
                .hasMessageContaining("Only ShardedRepository is permitted");
    }

    @Test
    void shouldThrowWhenMethodLevelShardedServiceInjectsNonShardedRepositoryInStrictMode() {
        context = new AnnotationConfigApplicationContext();
        CentralBillingPlanRepo mockBillingRepo = mock(CentralBillingPlanRepo.class);
        context.registerBean("billingRepo", CentralBillingPlanRepo.class, () -> mockBillingRepo);
        context.registerBean("invalidMethodService", InvalidMethodLevelShardedService.class, () -> new InvalidMethodLevelShardedService(mockBillingRepo));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.STRICT);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, null);

        assertThatThrownBy(validator::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("FRACTAL REPOSITORY VALIDATION FAILURE")
                .hasMessageContaining("InvalidMethodLevelShardedService")
                .hasMessageContaining("CentralBillingPlanRepo");
    }

    @Test
    void shouldPassWhenNonShardedRepositoryIsExplicitlyAllowed() {
        context = new AnnotationConfigApplicationContext();
        CentralBillingPlanRepo mockBillingRepo = mock(CentralBillingPlanRepo.class);
        context.registerBean("billingRepo", CentralBillingPlanRepo.class, () -> mockBillingRepo);
        context.registerBean("invalidService", InvalidClassLevelShardedService.class, () -> new InvalidClassLevelShardedService(mockBillingRepo));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.STRICT);
        properties.getValidation().setAllowedNonShardedRepositories(List.of("CentralBillingPlanRepo"));

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, null);
        // Passes because CentralBillingPlanRepo is whitelisted
        validator.validate();
    }

    @Test
    void shouldPassWhenShardedServiceOnlyInjectsShardedRepository() {
        context = new AnnotationConfigApplicationContext();
        ValidAccountShardedRepo mockAccountRepo = mock(ValidAccountShardedRepo.class);
        context.registerBean("accountRepo", ValidAccountShardedRepo.class, () -> mockAccountRepo);
        context.registerBean("validService", ValidShardedService.class, () -> new ValidShardedService(mockAccountRepo));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.STRICT);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, null);
        validator.validate();
    }

    @Test
    void shouldPassWhenCoordinatorServiceWithoutShardedAnnotationInjectsStandardRepository() {
        context = new AnnotationConfigApplicationContext();
        CentralBillingPlanRepo mockBillingRepo = mock(CentralBillingPlanRepo.class);
        context.registerBean("billingRepo", CentralBillingPlanRepo.class, () -> mockBillingRepo);
        context.registerBean("coordService", CoordinatorService.class, () -> new CoordinatorService(mockBillingRepo));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.STRICT);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, null);
        // Coordinator service has no @Sharded annotation, so standard repository is permitted
        validator.validate();
    }

    @Test
    void shouldWarnOnlyInWarnMode() {
        context = new AnnotationConfigApplicationContext();
        CentralBillingPlanRepo mockBillingRepo = mock(CentralBillingPlanRepo.class);
        context.registerBean("billingRepo", CentralBillingPlanRepo.class, () -> mockBillingRepo);
        context.registerBean("invalidService", InvalidClassLevelShardedService.class, () -> new InvalidClassLevelShardedService(mockBillingRepo));
        context.refresh();

        FractalProperties properties = new FractalProperties();
        properties.getValidation().setRepositoryEnforcement(FractalProperties.ValidationProperties.EnforcementMode.WARN);

        ShardedRepositoryStartupValidator validator = new ShardedRepositoryStartupValidator(context, properties, null);
        // Logs warning, does not throw
        validator.validate();
    }
}
