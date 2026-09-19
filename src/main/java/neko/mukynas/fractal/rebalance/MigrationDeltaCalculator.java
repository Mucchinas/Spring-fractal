package neko.mukynas.fractal.rebalance;

import neko.mukynas.fractal.config.FractalProperties;
import neko.mukynas.fractal.core.ConsistentHashRouter;
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

    /**
     * DTO per rappresentare un singolo utente/tenant da spostare
     */
    public record MigrationAction(String userId, String sourceShard, String targetShard) {}

    /**
     * Calcola la differenza tra il vecchio cluster (DB) e il nuovo cluster (YAML)
     * e identifica esattamente quali utenti devono essere migrati.
     *
     * @param dbShards Gli shard attualmente noti al database (prima della modifica)
     * @param yamlShards Tutti gli shard (inclusi quelli appena aggiunti) presenti nello YAML
     * @param virtualNodes Il numero di nodi virtuali configurato
     * @return Lista di azioni di migrazione
     */
    public List<MigrationAction> calculateDelta(List<String> dbShards, Set<String> yamlShards, int virtualNodes) {

        // 1. Costruiamo due "fotografie" dell'algoritmo di routing
        ConsistentHashRouter oldRouter = new ConsistentHashRouter(dbShards, virtualNodes);
        ConsistentHashRouter newRouter = new ConsistentHashRouter(yamlShards, virtualNodes);

        List<MigrationAction> actions = new ArrayList<>();

        // 2. Estraiamo TUTTI gli ID degli utenti dal DB primario.
        // NOTA: Se hai milioni di righe, in futuro questo blocco andrà paginato (es. usando un ResultSetExtractor o LIMIT/OFFSET).
        // Per ora raccogliamo gli ID in stream.
        String sql = "SELECT " + rootIdColumn + " FROM " + rootTable;

        primaryJdbcTemplate.query(sql, rs -> {
            String userId = rs.getString(1);

            // 3. Il cuore matematico: chiediamo ai due anelli dove si trova l'utente
            String currentShard = oldRouter.routeNode(userId);
            String futureShard = newRouter.routeNode(userId);

            // 4. Se la destinazione cambia, abbiamo trovato un utente da migrare!
            if (currentShard != null && !currentShard.equals(futureShard)) {
                actions.add(new MigrationAction(userId, currentShard, futureShard));
            }
        });

        return actions;
    }
}