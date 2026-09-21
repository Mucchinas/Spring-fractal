package io.github.mucchinas.fractal.aop;

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
import org.springframework.stereotype.Service;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:class_primary;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",

        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:class_shard1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",

        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:class_shard2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class ClassLevelShardedAspectTest {

    @Autowired
    private ClassLevelAnnotatedService service;

    @AfterEach
    void tearDown() {
        ShardContextHolder.clear();
    }

    @Test
    void shouldRouteMethodsWhenAnnotationIsAtClassLevel() {
        String shard1 = service.unannotatedMethod("user-alpha");
        assertThat(shard1).isNotBlank().startsWith("shard-");
        assertThat(ShardContextHolder.getShard()).isNull();

        String shard2 = service.anotherUnannotatedMethod("user-alpha");
        assertThat(shard2).isEqualTo(shard1);
        assertThat(ShardContextHolder.getShard()).isNull();
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({ClassLevelAnnotatedService.class, FractalAutoConfiguration.class})
    static class ClassLevelApp {}

    @Service
    @Sharded(key = "#userId")
    static class ClassLevelAnnotatedService {

        public String unannotatedMethod(String userId) {
            return ShardContextHolder.getShard();
        }

        public String anotherUnannotatedMethod(String userId) {
            return ShardContextHolder.getShard();
        }
    }
}
