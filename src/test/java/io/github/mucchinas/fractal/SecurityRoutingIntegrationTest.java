package io.github.mucchinas.fractal;

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
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:primarydb2",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",
        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:shard1db2",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",
        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:shard2db2",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class SecurityRoutingIntegrationTest {

    @Autowired
    private SecuredBusinessService businessService;

    @BeforeEach
    @AfterEach
    void cleanUp() {
        // Pulizia scrupolosa di ThreadLocal e SecurityContext tra un test e l'altro
        ShardContextHolder.clear();
        SecurityContextHolder.clearContext();
    }

    @Test
    void shouldExtractKeyFromJwtSecurityContext() {
        // Arrange: Creiamo un finto token JWT con un "sub" (Subject)
        String userId = "auth0-user-999";
        Jwt jwt = Jwt.withTokenValue("finto-token-jwt")
                .header("alg", "none")
                .claim("sub", userId)
                .build();

        JwtAuthenticationToken authentication = new JwtAuthenticationToken(jwt, List.of());

        // Inseriamo il token nel contesto di Spring Security del thread corrente
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // Act: Chiamiamo un metodo che NON usa SpEL. L'Aspect dovrà pescare il JWT
        String targetShard = businessService.eseguiQuerySicura();

        // Assert
        assertThat(targetShard)
                .isNotBlank()
                .startsWith("shard-"); // Ha instradato con successo!

        System.out.println("Utente JWT '" + userId + "' instradato automaticamente su: " + targetShard);
    }

    @Test
    void shouldThrowExceptionWhenNoKeyIsFound() {
        // Arrange: SecurityContext vuoto (nessun login)

        // Act & Assert: Chiamiamo un metodo senza SpEL fallback.
        // Ci aspettiamo che l'Aspect sollevi un'eccezione chiara per bloccare l'operazione
        assertThatThrownBy(() -> businessService.eseguiQuerySicura())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Impossibile determinare la chiave di sharding");
    }

    @Test
    void shouldClearThreadLocalEvenWhenExceptionOccurs() {
        // Arrange: Mettiamo un JWT valido per superare i controlli AOP
        Jwt jwt = Jwt.withTokenValue("finto").header("alg", "none").claim("sub", "user-1").build();
        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt, List.of()));

        // Act & Assert: Il metodo di business lancia un'eccezione Runtime.
        assertThatThrownBy(() -> businessService.queryCheFallisce())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Errore DB!");

        // Assert CRITICO: Nonostante l'esplosione del metodo, il blocco "finally"
        // dell'Aspect DEVE aver svuotato il contesto. Se questo fallisce, abbiamo un memory leak!
        assertThat(ShardContextHolder.getShard()).isNull();
    }


    // =================================================================
    // SETUP DELLA FALSA APPLICAZIONE PER IL TEST
    // =================================================================

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({SecuredBusinessService.class, FractalAutoConfiguration.class})
    static class DummyApp { }

    @Service
    static class SecuredBusinessService {

        // Nota: non abbiamo messo l'attributo "key" (SpEL) qui!
        @Sharded
        public String eseguiQuerySicura() {
            return ShardContextHolder.getShard();
        }

        @Sharded
        public String queryCheFallisce() {
            throw new RuntimeException("Errore DB!");
        }
    }
}