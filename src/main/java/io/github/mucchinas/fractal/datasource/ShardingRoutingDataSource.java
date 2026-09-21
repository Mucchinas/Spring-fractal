package io.github.mucchinas.fractal.datasource;

import io.github.mucchinas.fractal.core.ShardContextHolder;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ShardingRoutingDataSource extends AbstractRoutingDataSource implements DisposableBean {

    private DataSource primaryDataSource;
    private Map<String, DataSource> shardDataSources = Collections.emptyMap();
    private Map<Object, Object> rawTargetDataSources = Collections.emptyMap();
    private Object rawDefaultTargetDataSource;

    @Override
    public void setTargetDataSources(Map<Object, Object> targetDataSources) {
        super.setTargetDataSources(targetDataSources);
        this.rawTargetDataSources = targetDataSources != null ? new HashMap<>(targetDataSources) : Collections.emptyMap();
    }

    @Override
    public void setDefaultTargetDataSource(Object defaultTargetDataSource) {
        super.setDefaultTargetDataSource(defaultTargetDataSource);
        this.rawDefaultTargetDataSource = defaultTargetDataSource;
    }

    public DataSource getPrimaryDataSource() {
        return primaryDataSource;
    }

    public void setPrimaryDataSource(DataSource primaryDataSource) {
        this.primaryDataSource = primaryDataSource;
    }

    public Map<String, DataSource> getShardDataSources() {
        return shardDataSources;
    }

    public void setShardDataSources(Map<String, DataSource> shardDataSources) {
        this.shardDataSources = shardDataSources != null ? Collections.unmodifiableMap(new HashMap<>(shardDataSources)) : Collections.emptyMap();
    }

    @Override
    protected Object determineCurrentLookupKey() {
        return ShardContextHolder.getShard();
    }

    @Override
    public void destroy() {
        for (Object ds : rawTargetDataSources.values()) {
            closeDataSource(ds);
        }
        if (rawDefaultTargetDataSource != null && !rawTargetDataSources.containsValue(rawDefaultTargetDataSource)) {
            closeDataSource(rawDefaultTargetDataSource);
        }
    }

    private void closeDataSource(Object ds) {
        if (ds instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ignored) {
            }
        }
    }
}