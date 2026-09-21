package io.github.mucchinas.fractal.aop;

import io.github.mucchinas.fractal.annotation.ShardedBroadcast;
import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.annotation.AnnotationUtils;
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

    @Around("@annotation(io.github.mucchinas.fractal.annotation.ShardedBroadcast) || @within(io.github.mucchinas.fractal.annotation.ShardedBroadcast)")
    public Object broadcast(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        ShardedBroadcast shardedBroadcast = AnnotationUtils.findAnnotation(signature.getMethod(), ShardedBroadcast.class);
        if (shardedBroadcast == null && joinPoint.getTarget() != null) {
            shardedBroadcast = AnnotationUtils.findAnnotation(AopUtils.getTargetClass(joinPoint.getTarget()), ShardedBroadcast.class);
        }
        boolean includePrimary = shardedBroadcast == null || shardedBroadcast.includePrimary();

        String previousShard = ShardContextHolder.getShard();
        try {
            Object result = null;
            if (includePrimary) {
                ShardContextHolder.clear();
                result = joinPoint.proceed();
            }
            for (String shardName : shardNames) {
                ShardContextHolder.setShard(shardName);
                Object shardResult = joinPoint.proceed();
                if (!includePrimary && result == null) {
                    result = shardResult;
                }
            }
            return result;
        } finally {
            if (previousShard != null) {
                ShardContextHolder.setShard(previousShard);
            } else {
                ShardContextHolder.clear();
            }
        }
    }
}
