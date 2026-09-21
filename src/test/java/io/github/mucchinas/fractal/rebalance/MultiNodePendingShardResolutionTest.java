package io.github.mucchinas.fractal.rebalance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

class MultiNodePendingShardResolutionTest {

    private DataSource sharedPrimaryDs;
    private JdbcTemplate primaryJdbc;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl("jdbc:h2:mem:multinode_shard_res_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        ds.setUsername("sa");
        ds.setPassword("");
        this.sharedPrimaryDs = ds;

        this.primaryJdbc = new JdbcTemplate(ds);
        this.primaryJdbc.execute("DROP ALL OBJECTS");
    }

    @AfterEach
    void tearDown() {
        primaryJdbc.execute("DROP ALL OBJECTS");
    }

    @Test
    void shouldResolveSourceShardFromDbAcrossDifferentPods() {
        // Pod 1 initializes topology and records pending migration
        TopologyManager pod1 = new TopologyManager(sharedPrimaryDs);
        pod1.initializeSchema();
        pod1.recordPendingMigration("tenant-101", "shard-source", "shard-target");

        // Pod 2 has separate in-memory caches and did NOT record the migration locally
        TopologyManager pod2 = new TopologyManager(sharedPrimaryDs);

        // Pod 2 queries for the pending source shard - it must fetch from shared DB
        String resolvedSource = pod2.getPendingSourceShard("tenant-101");
        assertThat(resolvedSource).isEqualTo("shard-source");

        // When pod 1 completes the migration
        pod1.completePendingMigration("tenant-101");

        // Pod 2 should no longer resolve a pending source shard
        assertThat(pod2.getPendingSourceShard("tenant-101")).isNull();
    }
}
