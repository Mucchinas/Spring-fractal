package io.github.mucchinas.fractal.aop;

import io.github.mucchinas.fractal.annotation.ShardedBroadcast;
import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.core.annotation.Order;

import java.util.Set;

@Aspect
@Order(0)
public class ShardedBroadcastAspect {

    private final Set<String> shardNames;

    public ShardedBroadcastAspect(FractalProperties properties) {
        this.shardNames = properties != null ? properties.getActiveShardNames() : Set.of();
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
        if (includePrimary) {
            ShardContextHolder.clear();
            result = joinPoint.proceed();
        }
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
