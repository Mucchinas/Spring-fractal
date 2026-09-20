package io.github.mucchinas.fractal;

import io.github.mucchinas.fractal.annotation.Sharded;
import io.github.mucchinas.fractal.config.FractalAutoConfiguration;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:primarydb",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",

        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:shard1db",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",

        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:shard2db",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class RoutingIntegrationTest {

    @Autowired
    private DummyBusinessService businessService;

    @Autowired
    private DataSource routingDataSource;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        JdbcTemplate jdbc = new JdbcTemplate(routingDataSource);

        // Seed primary
        ShardContextHolder.clear();
        jdbc.execute("CREATE TABLE IF NOT EXISTS test_marker (id VARCHAR(50) PRIMARY KEY, marker VARCHAR(50))");
        jdbc.execute("MERGE INTO test_marker KEY(id) VALUES ('fixed', 'from-primary')");

        // Seed shard-1
        ShardContextHolder.setShard("shard-1");
        jdbc.execute("CREATE TABLE IF NOT EXISTS test_marker (id VARCHAR(50) PRIMARY KEY, marker VARCHAR(50))");
        jdbc.execute("MERGE INTO test_marker KEY(id) VALUES ('fixed', 'from-shard-1')");

        // Seed shard-2
        ShardContextHolder.setShard("shard-2");
        jdbc.execute("CREATE TABLE IF NOT EXISTS test_marker (id VARCHAR(50) PRIMARY KEY, marker VARCHAR(50))");
        jdbc.execute("MERGE INTO test_marker KEY(id) VALUES ('fixed', 'from-shard-2')");

        ShardContextHolder.clear();
    }

    @AfterEach
    void tearDown() {
        ShardContextHolder.clear();
    }

    @Test
    void shouldRouteToCorrectShardBasedOnSpelAndExecuteOnTargetDb() {
        String userA = "lorenzo";
        String userB = "mario";

        String shardUsatoPerA = businessService.eseguiQuery(userA);
        String shardUsatoPerB = businessService.eseguiQuery(userB);

        assertThat(shardUsatoPerA).isNotBlank().startsWith("shard-");
        assertThat(shardUsatoPerB).isNotBlank().startsWith("shard-");
        assertThat(ShardContextHolder.getShard()).isNull();

        // Verify physical database reading matches routed shard
        String markerA = businessService.readMarker(userA);
        assertThat(markerA).isEqualTo("from-" + shardUsatoPerA);

        String markerB = businessService.readMarker(userB);
        assertThat(markerB).isEqualTo("from-" + shardUsatoPerB);
    }

    @Test
    void shouldFallbackToPrimaryWhenMethodIsNotSharded() {
        String markerDefault = businessService.readMarkerWithoutSharding();
        assertThat(markerDefault).isEqualTo("from-primary");
        assertThat(ShardContextHolder.getShard()).isNull();
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({DummyBusinessService.class, FractalAutoConfiguration.class})
    static class DummyApp { }

    @Service
    static class DummyBusinessService {

        private final JdbcTemplate jdbcTemplate;

        DummyBusinessService(DataSource dataSource) {
            this.jdbcTemplate = new JdbcTemplate(dataSource);
        }

        @Sharded(key = "#userId")
        public String eseguiQuery(String userId) {
            jdbcTemplate.execute("SELECT 1");
            return ShardContextHolder.getShard();
        }

        @Sharded(key = "#userId")
        public String readMarker(String userId) {
            return jdbcTemplate.queryForObject("SELECT marker FROM test_marker WHERE id = 'fixed'", String.class);
        }

        public String readMarkerWithoutSharding() {
            return jdbcTemplate.queryForObject("SELECT marker FROM test_marker WHERE id = 'fixed'", String.class);
        }
    }
}