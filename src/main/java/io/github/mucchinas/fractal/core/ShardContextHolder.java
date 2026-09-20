package io.github.mucchinas.fractal.core;

public class ShardContextHolder {

    private static final ThreadLocal<String> CONTEXT = new ThreadLocal<>();

    public static void setShard(String shardId) {
        CONTEXT.set(shardId);
    }

    public static String getShard() {
        return CONTEXT.get();
    }

    public static void clear() {
        CONTEXT.remove();
    }
}