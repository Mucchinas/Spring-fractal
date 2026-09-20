package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.annotation.Sharded;
import io.github.mucchinas.fractal.config.FractalAutoConfiguration;
import io.github.mucchinas.fractal.core.ConsistentHashRouter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "fractal.sharding.primary.drain=true",
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:boot_drain_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",
        "fractal.sharding.rebalancer.enabled=true",
        "fractal.sharding.rebalancer.root-table=accounts",
        "fractal.sharding.rebalancer.root-id-column=id",
        "fractal.sharding.rebalancer.status-column=status",
        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:boot_drain_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",
        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:boot_drain_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class PrimaryDrainSpringBootTest {

    @SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
    @Import(FractalAutoConfiguration.class)
    static class TestApplication {
        @org.springframework.context.annotation.Bean
        public ShardedOrderService orderService(DataSource dataSource) {
            return new ShardedOrderService(dataSource);
        }
    }

    @Service
    static class ShardedOrderService {
        private final DataSource dataSource;

        public ShardedOrderService(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Sharded(key = "#accountId")
        public int getOrderCount(String accountId) {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            Integer count = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM orders WHERE account_id = ?",
                    Integer.class, accountId);
            return count != null ? count : 0;
        }
    }

    @Autowired
    private TopologyManager topologyManager;

    @Autowired
    private ConsistentHashRouter router;

    @Autowired
    private ShardedOrderService orderService;

    @Autowired(required = false)
    private ThreadPoolTaskExecutor fractalRebalanceExecutor;

    @BeforeAll
    static void seedPrimaryAndShards() {
        DriverManagerDataSource primaryDs = new DriverManagerDataSource();
        primaryDs.setDriverClassName("org.h2.Driver");
        primaryDs.setUrl("jdbc:h2:mem:boot_drain_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        primaryDs.setUsername("sa");
        primaryDs.setPassword("");
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);
        primaryJdbc.execute("DROP ALL OBJECTS");
        primaryJdbc.execute("CREATE TABLE accounts (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), status VARCHAR(50) NOT NULL)");
        primaryJdbc.execute("CREATE TABLE orders (id VARCHAR(255) PRIMARY KEY, account_id VARCHAR(255), amount DOUBLE PRECISION, FOREIGN KEY (account_id) REFERENCES accounts(id))");
        primaryJdbc.execute("INSERT INTO accounts VALUES ('acc-1', 'Account 1', 'ACTIVE'), ('acc-2', 'Account 2', 'ACTIVE')");
        primaryJdbc.execute("INSERT INTO orders VALUES ('ord-1', 'acc-1', 100.0), ('ord-2', 'acc-2', 250.0)");

        for (String url : List.of(
                "jdbc:h2:mem:boot_drain_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
                "jdbc:h2:mem:boot_drain_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1")) {
            DriverManagerDataSource shardDs = new DriverManagerDataSource();
            shardDs.setDriverClassName("org.h2.Driver");
            shardDs.setUrl(url);
            shardDs.setUsername("sa");
            shardDs.setPassword("");
            JdbcTemplate shardJdbc = new JdbcTemplate(shardDs);
            shardJdbc.execute("DROP ALL OBJECTS");
            shardJdbc.execute("CREATE TABLE accounts (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), status VARCHAR(50) NOT NULL)");
            shardJdbc.execute("CREATE TABLE orders (id VARCHAR(255) PRIMARY KEY, account_id VARCHAR(255), amount DOUBLE PRECISION, FOREIGN KEY (account_id) REFERENCES accounts(id))");
        }
    }

    @Test
    void shouldAutomaticallyDrainPrimaryToShardsOnBoot() throws Exception {
        if (fractalRebalanceExecutor != null) {
            long deadline = System.currentTimeMillis() + 8000;
            while ((fractalRebalanceExecutor.getActiveCount() > 0 || fractalRebalanceExecutor.getThreadPoolExecutor().getQueue().size() > 0)
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
        }

        // 1. Verify primary is marked as DRAINED
        assertThat(topologyManager.isPrimaryDrained()).isTrue();

        // 2. Verify worker topology has shard-1 and shard-2
        assertThat(topologyManager.getKnownShardsFromDb()).containsExactlyInAnyOrder("shard-1", "shard-2");

        // 3. Verify primary orders table is drained (count = 0)
        DriverManagerDataSource primaryDs = new DriverManagerDataSource();
        primaryDs.setDriverClassName("org.h2.Driver");
        primaryDs.setUrl("jdbc:h2:mem:boot_drain_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        primaryDs.setUsername("sa");
        primaryDs.setPassword("");
        JdbcTemplate primaryJdbc = new JdbcTemplate(primaryDs);

        Integer primaryOrders = primaryJdbc.queryForObject("SELECT COUNT(*) FROM orders", Integer.class);
        assertThat(primaryOrders).isEqualTo(0);

        // 4. Verify primary root table STILL contains the accounts with ACTIVE status
        Integer primaryAccounts = primaryJdbc.queryForObject("SELECT COUNT(*) FROM accounts", Integer.class);
        assertThat(primaryAccounts).isEqualTo(2);

        String status1 = primaryJdbc.queryForObject("SELECT status FROM accounts WHERE id = 'acc-1'", String.class);
        String status2 = primaryJdbc.queryForObject("SELECT status FROM accounts WHERE id = 'acc-2'", String.class);
        assertThat(status1).isEqualTo("ACTIVE");
        assertThat(status2).isEqualTo("ACTIVE");

        // 5. Verify total orders across shard-1 and shard-2 equals 2
        DriverManagerDataSource shard1Ds = new DriverManagerDataSource("jdbc:h2:mem:boot_drain_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1", "sa", "");
        DriverManagerDataSource shard2Ds = new DriverManagerDataSource("jdbc:h2:mem:boot_drain_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1", "sa", "");

        int shard1Orders = new JdbcTemplate(shard1Ds).queryForObject("SELECT COUNT(*) FROM orders", Integer.class);
        int shard2Orders = new JdbcTemplate(shard2Ds).queryForObject("SELECT COUNT(*) FROM orders", Integer.class);
        assertThat(shard1Orders + shard2Orders).isEqualTo(2);

        // 6. Verify sharded service query routes cleanly to the correct shard
        assertThat(orderService.getOrderCount("acc-1")).isEqualTo(1);
        assertThat(orderService.getOrderCount("acc-2")).isEqualTo(1);
    }
}
