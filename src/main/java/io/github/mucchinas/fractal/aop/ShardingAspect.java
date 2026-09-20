package io.github.mucchinas.fractal.aop;

import io.github.mucchinas.fractal.annotation.Sharded;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import io.github.mucchinas.fractal.core.ShardingKeyExtractor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.annotation.Order;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.exception.TenantMigratingException;
import io.github.mucchinas.fractal.rebalance.TopologyManager;

@Aspect
@Order(1)
public class ShardingAspect {

    private final ConsistentHashRouter router;
    private final ObjectProvider<ShardingKeyExtractor> keyExtractors;
    private final ObjectProvider<TopologyManager> topologyManagerProvider;
    private final ObjectProvider<FractalProperties> propertiesProvider;
    private final ExpressionParser parser = new SpelExpressionParser();

    public ShardingAspect(ConsistentHashRouter router, ObjectProvider<ShardingKeyExtractor> keyExtractors) {
        this(router, keyExtractors, null, null);
    }

    public ShardingAspect(ConsistentHashRouter router,
                          ObjectProvider<ShardingKeyExtractor> keyExtractors,
                          ObjectProvider<TopologyManager> topologyManagerProvider,
                          ObjectProvider<FractalProperties> propertiesProvider) {
        this.router = router;
        this.keyExtractors = keyExtractors;
        this.topologyManagerProvider = topologyManagerProvider;
        this.propertiesProvider = propertiesProvider;
    }

    @Around("@annotation(sharded)")
    public Object routeShard(ProceedingJoinPoint joinPoint, Sharded sharded) throws Throwable {
        String shardingKey = null;
        for (ShardingKeyExtractor extractor : keyExtractors.orderedStream().toList()) {
            shardingKey = extractor.extractKey();
            if (shardingKey != null && !shardingKey.isBlank()) {
                break;
            }
        }
        if ((shardingKey == null || shardingKey.isBlank()) && !sharded.key().isBlank()) {
            MethodSignature signature = (MethodSignature) joinPoint.getSignature();
            String[] paramNames = signature.getParameterNames();
            Object[] args = joinPoint.getArgs();

            EvaluationContext context = new StandardEvaluationContext();
            if (paramNames != null) {
                for (int i = 0; i < args.length; i++) {
                    context.setVariable(paramNames[i], args[i]);
                }
            }
            shardingKey = parser.parseExpression(sharded.key()).getValue(context, String.class);
        }
        if (shardingKey == null || shardingKey.isBlank()) {
            throw new IllegalStateException("Fractal Sharding: Impossibile determinare la chiave di sharding. " +
                    "SecurityContext vuoto/assente e parametro SpEL 'key' non fornito o nullo.");
        }
        TopologyManager topologyManager = topologyManagerProvider != null ? topologyManagerProvider.getIfAvailable() : null;
        FractalProperties properties = propertiesProvider != null ? propertiesProvider.getIfAvailable() : null;

        boolean rebalanceActive = topologyManager != null && properties != null
                && properties.getRebalancer().isEnabled()
                && topologyManager.isRebalanceActive();

        if (!rebalanceActive) {
            String targetShard = router.routeNode(shardingKey);
            ShardContextHolder.setShard(targetShard);
            try {
                return joinPoint.proceed();
            } finally {
                ShardContextHolder.clear();
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
            ShardContextHolder.clear();
        }
    }
}