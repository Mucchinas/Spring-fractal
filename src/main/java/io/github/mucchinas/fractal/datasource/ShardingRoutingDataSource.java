package io.github.mucchinas.fractal.datasource;

import io.github.mucchinas.fractal.core.ShardContextHolder;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class ShardingRoutingDataSource extends AbstractRoutingDataSource {

    @Override
    protected Object determineCurrentLookupKey() {
        return ShardContextHolder.getShard();
    }
}