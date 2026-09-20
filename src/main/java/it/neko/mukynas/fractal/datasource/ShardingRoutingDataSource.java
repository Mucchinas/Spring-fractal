package it.neko.mukynas.fractal.datasource;

import it.neko.mukynas.fractal.core.ShardContextHolder;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class ShardingRoutingDataSource extends AbstractRoutingDataSource {

    @Override
    protected Object determineCurrentLookupKey() {
        return ShardContextHolder.getShard();
    }
}