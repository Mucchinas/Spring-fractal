package io.github.mucchinas.fractal.aop;

import io.github.mucchinas.fractal.annotation.Sharded;
import io.github.mucchinas.fractal.config.FractalAutoConfiguration;
import io.github.mucchinas.fractal.core.ShardContextHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:nested_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",

        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:nested_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",

        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:nested_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class NestedShardedContextTest {

    @Autowired
    private OuterService outerService;

    @Autowired
    private DataSource routingDataSource;

    @BeforeEach
    void setUp() {
        JdbcTemplate jdbc = new JdbcTemplate(routingDataSource);

        ShardContextHolder.setShard("shard-1");
        jdbc.execute("CREATE TABLE IF NOT EXISTS marker (id INT PRIMARY KEY, val VARCHAR(50))");
        jdbc.execute("MERGE INTO marker KEY(id) VALUES (1, 'from-shard-1')");

        ShardContextHolder.setShard("shard-2");
        jdbc.execute("CREATE TABLE IF NOT EXISTS marker (id INT PRIMARY KEY, val VARCHAR(50))");
        jdbc.execute("MERGE INTO marker KEY(id) VALUES (1, 'from-shard-2')");

        ShardContextHolder.clear();
    }

    @AfterEach
    void tearDown() {
        ShardContextHolder.clear();
    }

    @Test
    void shouldRestoreOuterShardContextAfterInnerShardedMethodCompletes() {
        // "user-1" routes to one shard, "user-2" might route to the other
        // NestedInvocationResult tracks: outerShardAtStart, innerShard, outerShardAfterInner, markerAfterInner
        OuterService.NestedResult result = outerService.executeNested("user-1", "user-2");

        assertThat(result.outerShardAtStart()).isNotBlank();
        assertThat(result.innerShard()).isNotBlank();
        // The crucial assertion: outer shard must be restored after inner method exits!
        assertThat(result.outerShardAfterInner()).isEqualTo(result.outerShardAtStart());
        // Verify query executed after inner method actually read from the outer shard
        assertThat(result.markerAfterInner()).isEqualTo("from-" + result.outerShardAtStart());
        // ThreadLocal must be clean after the entire invocation completes
        assertThat(ShardContextHolder.getShard()).isNull();
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({OuterService.class, InnerService.class, FractalAutoConfiguration.class})
    static class NestedApp {}

    @Service
    static class OuterService {
        private final InnerService innerService;
        private final JdbcTemplate jdbcTemplate;

        OuterService(InnerService innerService, DataSource dataSource) {
            this.innerService = innerService;
            this.jdbcTemplate = new JdbcTemplate(dataSource);
        }

        @Sharded(key = "#outerUser")
        public NestedResult executeNested(String outerUser, String innerUser) {
            String outerAtStart = ShardContextHolder.getShard();
            String inner = innerService.executeInner(innerUser);
            String outerAfterInner = ShardContextHolder.getShard();
            String marker = jdbcTemplate.queryForObject("SELECT val FROM marker WHERE id = 1", String.class);

            return new NestedResult(outerAtStart, inner, outerAfterInner, marker);
        }

        record NestedResult(String outerShardAtStart, String innerShard, String outerShardAfterInner, String markerAfterInner) {}
    }

    @Service
    static class InnerService {
        @Sharded(key = "#innerUser")
        public String executeInner(String innerUser) {
            return ShardContextHolder.getShard();
        }
    }
}
