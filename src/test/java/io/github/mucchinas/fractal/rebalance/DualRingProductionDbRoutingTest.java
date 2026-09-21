package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.config.FractalProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

class DualRingProductionDbRoutingTest {

    private DataSource primaryDs;
    private JdbcTemplate primaryJdbc;
    private TopologyManager topologyManager;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl("jdbc:h2:mem:dual_ring_db_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        ds.setUsername("sa");
        ds.setPassword("");
        this.primaryDs = ds;

        this.primaryJdbc = new JdbcTemplate(ds);
        this.primaryJdbc.execute("DROP ALL OBJECTS");

        this.topologyManager = new TopologyManager(primaryDs);
        this.topologyManager.initializeSchema();
    }

    @AfterEach
    void tearDown() {
        primaryJdbc.execute("DROP ALL OBJECTS");
    }

    @Test
    void shouldNotTreatPendingMigrationInDbAsActivelyMigrating() {
        String tenantId = "tenant-dual-ring-1";

        // Step 1: Record planned migration (phase = PHASE_PENDING)
        topologyManager.recordPendingMigration(tenantId, "shard-1", "shard-2");

        // Step 2: Query DB check directly
        boolean isMigratingInDb = topologyManager.checkMigrationInPrimaryDb(tenantId);
        // PHASE_PENDING must NOT block queries!
        assertThat(isMigratingInDb).isFalse();

        // Step 3: Verify source shard can be resolved from DB or pending cache
        String sourceShard = topologyManager.getPendingSourceShard(tenantId);
        assertThat(sourceShard).isEqualTo("shard-1");

        // Step 4: When pod starts actively copying (phase = PHASE_COPYING)
        topologyManager.recordMigrationStart(tenantId, "shard-1", "shard-2");
        assertThat(topologyManager.checkMigrationInPrimaryDb(tenantId)).isTrue();
        assertThat(topologyManager.getMigrationPhase(tenantId)).isEqualTo(TopologyManager.PHASE_COPYING);

        // Step 5: When phase enters PHASE_PRUNING
        topologyManager.updateMigrationPhase(tenantId, TopologyManager.PHASE_PRUNING);
        assertThat(topologyManager.checkMigrationInPrimaryDb(tenantId)).isTrue();
        assertThat(topologyManager.getMigrationPhase(tenantId)).isEqualTo(TopologyManager.PHASE_PRUNING);

        // Step 6: When completed, record is removed and migration status is cleared
        topologyManager.completePendingMigration(tenantId);
        assertThat(topologyManager.checkMigrationInPrimaryDb(tenantId)).isFalse();
        assertThat(topologyManager.getPendingSourceShard(tenantId)).isNull();
    }
}
