package it.neko.mukynas.fractal.aop;

import it.neko.mukynas.fractal.annotation.Sharded;
import it.neko.mukynas.fractal.core.ConsistentHashRouter;
import it.neko.mukynas.fractal.core.ShardContextHolder;
import it.neko.mukynas.fractal.core.ShardingKeyExtractor;
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

import it.neko.mukynas.fractal.config.FractalProperties;
import it.neko.mukynas.fractal.exception.TenantMigratingException;
import it.neko.mukynas.fractal.rebalance.TopologyManager;

@Aspect
@Order(1) // CRITICO: Deve eseguire PRIMA dell'apertura della transazione Spring
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

        // 1. Tenta di estrarre la chiave tramite le strategie (es. JWT Security)
        for (ShardingKeyExtractor extractor : keyExtractors.orderedStream().toList()) {
            shardingKey = extractor.extractKey();
            if (shardingKey != null && !shardingKey.isBlank()) {
                break; // Chiave trovata, usciamo dal loop
            }
        }

        // 2. Fallback su SpEL se la chiave non è stata trovata (es. Job asincrono)
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

        // 3. Controllo finale
        if (shardingKey == null || shardingKey.isBlank()) {
            throw new IllegalStateException("Fractal Sharding: Impossibile determinare la chiave di sharding. " +
                    "SecurityContext vuoto/assente e parametro SpEL 'key' non fornito o nullo.");
        }

        // 3.5. In-flight request tracking and migration status check
        TopologyManager topologyManager = topologyManagerProvider != null ? topologyManagerProvider.getIfAvailable() : null;
        FractalProperties properties = propertiesProvider != null ? propertiesProvider.getIfAvailable() : null;

        if (topologyManager != null) {
            topologyManager.registerRequestStart(shardingKey);
        }

        try {
            if (topologyManager != null && properties != null && properties.getRebalancer().isEnabled()) {
                if (topologyManager.isTenantMigrating(shardingKey, properties.getRebalancer())) {
                    throw new TenantMigratingException(shardingKey);
                }
            }

            // 4. Routing verso il nodo virtuale
            String targetShard = router.routeNode(shardingKey);
            ShardContextHolder.setShard(targetShard);

            // 5. Eseguiamo la query sul DB corretto
            return joinPoint.proceed();
        } finally {
            if (topologyManager != null) {
                topologyManager.registerRequestEnd(shardingKey);
            }
            // 6. Pulizia ThreadLocal obbligatoria
            ShardContextHolder.clear();
        }
    }
}