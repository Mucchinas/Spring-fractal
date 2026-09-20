package it.neko.mukynas.fractal.rebalance;

import it.neko.mukynas.fractal.rebalance.ReplicaTableSynchronizer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicaTableSynchronizerTest {

    private DataSource primaryDs;
    private DataSource shard1Ds;
    private DataSource shard2Ds;

    private JdbcTemplate primaryJdbc;
    private JdbcTemplate shard1Jdbc;
    private JdbcTemplate shard2Jdbc;

    private ReplicaTableSynchronizer synchronizer;

    @BeforeEach
    void setUp() {
        primaryDs = createDataSource("jdbc:h2:mem:rep_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard1Ds = createDataSource("jdbc:h2:mem:rep_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        shard2Ds = createDataSource("jdbc:h2:mem:rep_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");

        primaryJdbc = new JdbcTemplate(primaryDs);
        shard1Jdbc = new JdbcTemplate(shard1Ds);
        shard2Jdbc = new JdbcTemplate(shard2Ds);

        String schema = "CREATE TABLE currencies (code VARCHAR(10) PRIMARY KEY, rate DECIMAL(10, 4));";
        primaryJdbc.execute("DROP ALL OBJECTS");
        shard1Jdbc.execute("DROP ALL OBJECTS");
        shard2Jdbc.execute("DROP ALL OBJECTS");

        primaryJdbc.execute(schema);
        shard1Jdbc.execute(schema);
        shard2Jdbc.execute(schema);

        primaryJdbc.update("INSERT INTO currencies (code, rate) VALUES ('EUR', 1.0)");
        primaryJdbc.update("INSERT INTO currencies (code, rate) VALUES ('USD', 1.08)");
        primaryJdbc.update("INSERT INTO currencies (code, rate) VALUES ('GBP', 0.85)");

        synchronizer = new ReplicaTableSynchronizer(primaryDs, Map.of("shard-1", shard1Ds, "shard-2", shard2Ds));
    }

    @AfterEach
    void tearDown() {
        primaryJdbc.execute("DROP ALL OBJECTS");
        shard1Jdbc.execute("DROP ALL OBJECTS");
        shard2Jdbc.execute("DROP ALL OBJECTS");
    }

    @Test
    void shouldSynchronizeReplicaTablesToAllShards() {
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM currencies", Integer.class));
        assertEquals(0, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM currencies", Integer.class));

        synchronizer.syncAllReplicaTables(List.of("currencies"));

        assertEquals(3, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM currencies", Integer.class));
        assertEquals(3, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM currencies", Integer.class));

        assertEquals(1.08, shard1Jdbc.queryForObject("SELECT rate FROM currencies WHERE code = 'USD'", Double.class), 0.001);
        assertEquals(0.85, shard2Jdbc.queryForObject("SELECT rate FROM currencies WHERE code = 'GBP'", Double.class), 0.001);
    }

    @Test
    void shouldSynchronizeToSpecificNewShardDuringClusterExpansion() {
        assertEquals(0, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM currencies", Integer.class));

        synchronizer.syncAllReplicaTablesToShard("shard-1", List.of("currencies"));

        assertEquals(3, shard1Jdbc.queryForObject("SELECT COUNT(*) FROM currencies", Integer.class));
        assertEquals(0, shard2Jdbc.queryForObject("SELECT COUNT(*) FROM currencies", Integer.class));
    }

    private DataSource createDataSource(String url) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl(url);
        ds.setUsername("sa");
        ds.setPassword("");
        return ds;
    }
}
