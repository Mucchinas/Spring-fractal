package neko.mukynas.fractal.aop;

import neko.mukynas.fractal.annotation.ShardedBroadcast;
import neko.mukynas.fractal.config.FractalProperties;
import neko.mukynas.fractal.core.ShardContextHolder;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.core.annotation.Order;

import java.util.Set;

/**
 * Intercepts methods annotated with {@link ShardedBroadcast} and replicates the operation
 * across the primary coordinator database and all configured physical shards.
 */
@Aspect
@Order(0)
public class ShardedBroadcastAspect {

    private final Set<String> shardNames;

    public ShardedBroadcastAspect(FractalProperties properties) {
        this.shardNames = properties.getShards() != null ? properties.getShards().keySet() : Set.of();
    }

    public ShardedBroadcastAspect(Set<String> shardNames) {
        this.shardNames = shardNames != null ? shardNames : Set.of();
    }

    @Around("@annotation(shardedBroadcast) || @within(shardedBroadcast)")
    public Object broadcast(ProceedingJoinPoint joinPoint, ShardedBroadcast shardedBroadcast) throws Throwable {
        if (shardedBroadcast == null) {
            shardedBroadcast = joinPoint.getTarget().getClass().getAnnotation(ShardedBroadcast.class);
        }
        boolean includePrimary = shardedBroadcast == null || shardedBroadcast.includePrimary();

        Object result = null;

        // 1. Execute on primary coordinator database if requested
        if (includePrimary) {
            ShardContextHolder.clear(); // null -> routes to default primary datasource
            result = joinPoint.proceed();
        }

        // 2. Replicate execution to all physical shards
        for (String shardName : shardNames) {
            try {
                ShardContextHolder.setShard(shardName);
                Object shardResult = joinPoint.proceed();
                if (!includePrimary && result == null) {
                    result = shardResult;
                }
            } finally {
                ShardContextHolder.clear();
            }
        }

        return result;
    }
}
