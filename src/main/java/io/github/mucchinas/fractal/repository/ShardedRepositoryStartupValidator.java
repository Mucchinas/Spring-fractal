package io.github.mucchinas.fractal.repository;

import io.github.mucchinas.fractal.annotation.Sharded;
import io.github.mucchinas.fractal.annotation.ShardedEntity;
import io.github.mucchinas.fractal.annotation.ShardedReplica;
import io.github.mucchinas.fractal.annotation.ShardedRoot;
import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.rebalance.EntityMetadataResult;
import io.github.mucchinas.fractal.rebalance.EntityTableMetadataResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.GenericTypeResolver;
import org.springframework.data.repository.Repository;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

public class ShardedRepositoryStartupValidator {

    private static final Logger log = LoggerFactory.getLogger(ShardedRepositoryStartupValidator.class);

    private final ApplicationContext applicationContext;
    private final FractalProperties properties;
    private final ObjectProvider<EntityTableMetadataResolver> metadataResolverProvider;

    public ShardedRepositoryStartupValidator(ApplicationContext applicationContext,
                                            FractalProperties properties,
                                            ObjectProvider<EntityTableMetadataResolver> metadataResolverProvider) {
        this.applicationContext = applicationContext;
        this.properties = properties;
        this.metadataResolverProvider = metadataResolverProvider;
    }

    public void validate() {
        FractalProperties.ValidationProperties validation = properties.getValidation();
        FractalProperties.ValidationProperties.EnforcementMode mode = validation.getRepositoryEnforcement();
        if (mode == FractalProperties.ValidationProperties.EnforcementMode.DISABLED) {
            return;
        }

        List<String> violations = new ArrayList<>();
        validateShardedRepositories(violations);
        validateShardedServices(violations);

        if (!violations.isEmpty()) {
            StringBuilder sb = new StringBuilder("\n");
            sb.append("====================================================================================================\n");
            sb.append("FRACTAL REPOSITORY VALIDATION ").append(mode == FractalProperties.ValidationProperties.EnforcementMode.STRICT ? "FAILURE" : "WARNING").append(":\n");
            sb.append("The following repository configuration issues were detected:\n");
            for (String v : violations) {
                sb.append("  - ").append(v).append("\n");
            }
            sb.append("To resolve:\n");
            sb.append("  1. Ensure entities managed by ShardedRepository are annotated with @ShardedRoot, @ShardedEntity, or @ShardedReplica.\n");
            sb.append("  2. Ensure @Sharded services only inject repositories extending ShardedRepository.\n");
            sb.append("  3. For coordinator-only repositories in @Sharded services, add them to 'fractal.sharding.validation.allowed-non-sharded-repositories'.\n");
            sb.append("====================================================================================================");

            if (mode == FractalProperties.ValidationProperties.EnforcementMode.STRICT) {
                throw new IllegalStateException(sb.toString());
            } else {
                log.warn("{}", sb.toString());
            }
        }
    }

    private void validateShardedRepositories(List<String> violations) {
        EntityTableMetadataResolver metadataResolver = metadataResolverProvider != null ? metadataResolverProvider.getIfAvailable() : null;
        EntityMetadataResult metadata = null;
        if (metadataResolver != null) {
            try {
                metadata = metadataResolver.resolve();
            } catch (Exception e) {
                log.debug("FRACTAL: Entity metadata resolution during repository validation: {}", e.getMessage());
            }
        }

        String[] repoBeanNames = applicationContext.getBeanNamesForType(ShardedRepository.class);
        for (String beanName : repoBeanNames) {
            Class<?> targetType = applicationContext.getType(beanName);
            if (targetType == null) {
                continue;
            }

            Class<?> entityClass = resolveDomainEntityClass(targetType);
            if (entityClass == null) {
                continue;
            }

            boolean isRoot = entityClass.isAnnotationPresent(ShardedRoot.class)
                    || (entityClass.isAnnotationPresent(ShardedEntity.class) && entityClass.getAnnotation(ShardedEntity.class).root());
            boolean isReplica = entityClass.isAnnotationPresent(ShardedReplica.class);
            boolean isShardedChild = entityClass.isAnnotationPresent(ShardedEntity.class);

            if (!isRoot && !isReplica && !isShardedChild) {
                violations.add("Repository '" + targetType.getName() + "' extends ShardedRepository<" + entityClass.getSimpleName()
                        + ", ...>, but entity '" + entityClass.getName()
                        + "' is NOT sharded! It must be annotated with @ShardedRoot, @ShardedEntity (with @ShardedKey), or @ShardedReplica.");
            } else if (isShardedChild && !isRoot) {
                if (metadata != null) {
                    String tableName = metadataResolver.resolveTableName(entityClass);
                    boolean inPlan = metadata.shardedTables() != null && metadata.shardedTables().contains(tableName.toLowerCase());
                    if (!inPlan) {
                        violations.add("Repository '" + targetType.getName() + "' manages @ShardedEntity '" + entityClass.getName()
                                + "', but this entity has no reachability path to @ShardedRoot!");
                    }
                }
            }
        }
    }

    private void validateShardedServices(List<String> violations) {
        Set<String> allowedRepos = new HashSet<>(properties.getValidation().getAllowedNonShardedRepositories());
        String[] allBeanNames = applicationContext.getBeanDefinitionNames();

        for (String beanName : allBeanNames) {
            Object bean;
            try {
                bean = applicationContext.getBean(beanName);
            } catch (Exception e) {
                continue;
            }

            Class<?> targetClass = AopUtils.getTargetClass(bean);
            if (!isShardedService(targetClass)) {
                continue;
            }

            Class<?> current = targetClass;
            while (current != null && !current.equals(Object.class)) {
                for (Field field : current.getDeclaredFields()) {
                    if (Repository.class.isAssignableFrom(field.getType())) {
                        Class<?> repoType = field.getType();
                        if (!ShardedRepository.class.isAssignableFrom(repoType)) {
                            boolean isAllowed = allowedRepos.contains(repoType.getSimpleName())
                                    || allowedRepos.contains(repoType.getName())
                                    || allowedRepos.contains(field.getName());
                            if (!isAllowed) {
                                violations.add("@Sharded service '" + targetClass.getName()
                                        + "' injects non-sharded repository '" + repoType.getName()
                                        + "' on field '" + field.getName() + "'. Inside @Sharded methods, queries execute on physical worker shards. Only ShardedRepository is permitted.");
                            }
                        }
                    }
                }
                current = current.getSuperclass();
            }
        }
    }

    private boolean isShardedService(Class<?> clazz) {
        if (clazz.isAnnotationPresent(Sharded.class)) {
            return true;
        }
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(Sharded.class)) {
                return true;
            }
        }
        return false;
    }

    private Class<?> resolveDomainEntityClass(Class<?> repoClass) {
        for (Class<?> iface : repoClass.getInterfaces()) {
            if (ShardedRepository.class.isAssignableFrom(iface)) {
                Class<?>[] args = GenericTypeResolver.resolveTypeArguments(iface, ShardedRepository.class);
                if (args != null && args.length > 0) {
                    return args[0];
                }
            }
        }
        Class<?>[] args = GenericTypeResolver.resolveTypeArguments(repoClass, ShardedRepository.class);
        if (args != null && args.length > 0) {
            return args[0];
        }
        return null;
    }
}
