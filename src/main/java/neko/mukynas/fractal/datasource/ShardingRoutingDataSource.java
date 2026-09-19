package neko.mukynas.fractal.datasource;

import neko.mukynas.fractal.core.ShardContextHolder;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class ShardingRoutingDataSource extends AbstractRoutingDataSource {

    @Override
    protected Object determineCurrentLookupKey() {
        return ShardContextHolder.getShard();
    }
}