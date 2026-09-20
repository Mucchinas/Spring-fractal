package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MigrationDeltaCalculator {

    private final JdbcTemplate primaryJdbcTemplate;
    private final String rootTable;
    private final String rootIdColumn;

    public MigrationDeltaCalculator(DataSource primaryDataSource, FractalProperties.RebalancerProperties props) {
        this.primaryJdbcTemplate = new JdbcTemplate(primaryDataSource);
        this.rootTable = props.getRootTable();
        this.rootIdColumn = props.getRootIdColumn();
    }

    public record MigrationAction(String userId, String sourceShard, String targetShard) {}

    public List<MigrationAction> calculateDelta(List<String> dbShards, Set<String> yamlShards, int virtualNodes) {
        ConsistentHashRouter oldRouter = new ConsistentHashRouter(dbShards, virtualNodes);
        ConsistentHashRouter newRouter = new ConsistentHashRouter(yamlShards, virtualNodes);

        List<MigrationAction> actions = new ArrayList<>();
        String sql = "SELECT " + rootIdColumn + " FROM " + rootTable;

        primaryJdbcTemplate.query(sql, rs -> {
            String userId = rs.getString(1);
            String currentShard = oldRouter.routeNode(userId);
            String futureShard = newRouter.routeNode(userId);
            if (currentShard != null && !currentShard.equals(futureShard)) {
                actions.add(new MigrationAction(userId, currentShard, futureShard));
            }
        });

        return actions;
    }
}