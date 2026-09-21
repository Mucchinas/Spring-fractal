package io.github.mucchinas.fractal.aop;

import io.github.mucchinas.fractal.annotation.Sharded;
import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import io.github.mucchinas.fractal.core.ShardingKeyExtractor;
import io.github.mucchinas.fractal.exception.TenantMigratingException;
import io.github.mucchinas.fractal.provisioning.TenantProvisioner;
import io.github.mucchinas.fractal.rebalance.TopologyManager;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.Order;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Aspect
@Order(1)
public class ShardingAspect {

    private final ConsistentHashRouter router;
    private final ObjectProvider<ShardingKeyExtractor> keyExtractors;
    private final ObjectProvider<TopologyManager> topologyManagerProvider;
    private final ObjectProvider<FractalProperties> propertiesProvider;
    private final ObjectProvider<TenantProvisioner> tenantProvisionerProvider;
    private final ExpressionParser parser = new SpelExpressionParser();
    private final ConcurrentMap<String, Expression> expressionCache = new ConcurrentHashMap<>();
    private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

    public ShardingAspect(ConsistentHashRouter router, ObjectProvider<ShardingKeyExtractor> keyExtractors) {
        this(router, keyExtractors, null, null, null);
    }

    public ShardingAspect(ConsistentHashRouter router,
                          ObjectProvider<ShardingKeyExtractor> keyExtractors,
                          ObjectProvider<TopologyManager> topologyManagerProvider,
                          ObjectProvider<FractalProperties> propertiesProvider) {
        this(router, keyExtractors, topologyManagerProvider, propertiesProvider, null);
    }

    public ShardingAspect(ConsistentHashRouter router,
                          ObjectProvider<ShardingKeyExtractor> keyExtractors,
                          ObjectProvider<TopologyManager> topologyManagerProvider,
                          ObjectProvider<FractalProperties> propertiesProvider,
                          ObjectProvider<TenantProvisioner> tenantProvisionerProvider) {
        this.router = router;
        this.keyExtractors = keyExtractors;
        this.topologyManagerProvider = topologyManagerProvider;
        this.propertiesProvider = propertiesProvider;
        this.tenantProvisionerProvider = tenantProvisionerProvider;
    }

    @Around("@annotation(io.github.mucchinas.fractal.annotation.Sharded) || @within(io.github.mucchinas.fractal.annotation.Sharded)")
    public Object routeShard(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Sharded sharded = AnnotationUtils.findAnnotation(method, Sharded.class);
        if (sharded == null && joinPoint.getTarget() != null) {
            sharded = AnnotationUtils.findAnnotation(joinPoint.getTarget().getClass(), Sharded.class);
        }

        String shardingKey = null;
        for (ShardingKeyExtractor extractor : keyExtractors.orderedStream().toList()) {
            shardingKey = extractor.extractKey();
            if (shardingKey != null && !shardingKey.isBlank()) {
                break;
            }
        }
        if ((shardingKey == null || shardingKey.isBlank()) && sharded != null && !sharded.key().isBlank()) {
            String[] paramNames = parameterNameDiscoverer.getParameterNames(method);
            if (paramNames == null) {
                paramNames = signature.getParameterNames();
            }
            Object[] args = joinPoint.getArgs();

            EvaluationContext context = new StandardEvaluationContext();
            if (paramNames != null && args != null) {
                int limit = Math.min(paramNames.length, args.length);
                for (int i = 0; i < limit; i++) {
                    context.setVariable(paramNames[i], args[i]);
                }
            }
            Expression expr = expressionCache.computeIfAbsent(sharded.key(), parser::parseExpression);
            shardingKey = expr.getValue(context, String.class);
        }
        if (shardingKey == null || shardingKey.isBlank()) {
            throw new IllegalStateException("Fractal Sharding: Unable to determine sharding key (Impossibile determinare la chiave di sharding). " +
                    "SecurityContext empty/absent and SpEL parameter 'key' not provided or null.");
        }

        boolean provision = sharded != null && sharded.provision();
        if (provision && tenantProvisionerProvider != null) {
            TenantProvisioner provisioner = tenantProvisionerProvider.getIfAvailable();
            if (provisioner != null) {
                org.springframework.security.core.Authentication auth = null;
                try {
                    auth = org.springframework.security.core.context.SecurityContextHolder.getContext().getAuthentication();
                } catch (NoClassDefFoundError | Exception ignored) {
                }
                provisioner.ensureProvisioned(shardingKey, auth);
            }
        }

        TopologyManager topologyManager = topologyManagerProvider != null ? topologyManagerProvider.getIfAvailable() : null;
        FractalProperties properties = propertiesProvider != null ? propertiesProvider.getIfAvailable() : null;

        boolean rebalanceActive = topologyManager != null && properties != null
                && properties.getRebalancer().isEnabled()
                && topologyManager.isRebalanceActive();

        String previousShard = ShardContextHolder.getShard();

        if (!rebalanceActive) {
            String targetShard = router.routeNode(shardingKey);
            ShardContextHolder.setShard(targetShard);
            try {
                return joinPoint.proceed();
            } finally {
                if (previousShard != null) {
                    ShardContextHolder.setShard(previousShard);
                } else {
                    ShardContextHolder.clear();
                }
            }
        }

        topologyManager.registerRequestStart(shardingKey);
        try {
            if (topologyManager.isTenantMigrating(shardingKey, properties.getRebalancer())) {
                throw new TenantMigratingException(shardingKey);
            }
            String sourceOverride = topologyManager.getPendingSourceShard(shardingKey);
            String targetShard = (sourceOverride != null) ? sourceOverride : router.routeNode(shardingKey);
            ShardContextHolder.setShard(targetShard);
            return joinPoint.proceed();
        } finally {
            topologyManager.registerRequestEnd(shardingKey);
            if (previousShard != null) {
                ShardContextHolder.setShard(previousShard);
            } else {
                ShardContextHolder.clear();
            }
        }
    }
}