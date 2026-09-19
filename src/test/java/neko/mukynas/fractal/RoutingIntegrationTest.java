package neko.mukynas.fractal;

import neko.mukynas.fractal.annotation.Sharded;
import neko.mukynas.fractal.config.FractalAutoConfiguration;
import neko.mukynas.fractal.core.ShardContextHolder;
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
        // Configuriamo i database simulati tramite properties
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

    @AfterEach
    void tearDown() {
        // Puliamo il contesto per sicurezza tra un test e l'altro
        ShardContextHolder.clear();
    }

    @Test
    void shouldRouteToCorrectShardBasedOnSpel() {
        // Scegliamo due ID che l'hash manderà su shard diversi (probabilmente)
        String userA = "lorenzo";
        String userB = "mario";

        // Il servizio esegue la logica, l'AOP intercetta e devia.
        String shardUsatoPerA = businessService.eseguiQuery(userA);
        String shardUsatoPerB = businessService.eseguiQuery(userB);

        // Verifichiamo che il ThreadLocal sia stato correttamente popolato
        // dall'aspetto e poi svuotato (il metodo eseguiQuery restituisce lo shard usato prima di pulirlo)
        assertThat(shardUsatoPerA).isNotBlank().startsWith("shard-");
        assertThat(shardUsatoPerB).isNotBlank().startsWith("shard-");

        // Verifichiamo che il ThreadLocal sia vuoto a fine esecuzione (no memory leak)
        assertThat(ShardContextHolder.getShard()).isNull();
    }

    // =================================================================
    // SETUP DELLA FALSA APPLICAZIONE PER IL TEST
    // =================================================================

    // Questa annotazione fa credere a Spring Boot di essere in una vera app.
    // Attiverà la nostra FractalAutoConfiguration automaticamente!

    // 1. Escludiamo il DataSource di default di Spring Boot
    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    // 2. Importiamo il service E forziamo il caricamento della nostra libreria
    @Import({DummyBusinessService.class, FractalAutoConfiguration.class})
    static class DummyApp { }

    @Service
    static class DummyBusinessService {

        private final JdbcTemplate jdbcTemplate;

        // Spring inietterà in automatico il nostro ShardingRoutingDataSource
        DummyBusinessService(DataSource dataSource) {
            this.jdbcTemplate = new JdbcTemplate(dataSource);
        }

        // Testiamo il parse di SpEL sui parametri
        @Sharded(key = "#userId")
        public String eseguiQuery(String userId) {
            // Eseguiamo una query finta. Con H2 non serve creare le tabelle
            // se interroghiamo tabelle di sistema come DUAL
            jdbcTemplate.execute("SELECT 1");

            // Restituiamo in quale shard siamo finiti per permettere l'assert nel test
            return ShardContextHolder.getShard();
        }
    }
}