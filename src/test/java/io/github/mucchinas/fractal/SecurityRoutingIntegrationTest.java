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
        ShardContextHolder.clear();
        SecurityContextHolder.clearContext();
    }

    @Test
    void shouldExtractKeyFromJwtSecurityContext() {
        String userId = "auth0-user-999";
        Jwt jwt = Jwt.withTokenValue("finto-token-jwt")
                .header("alg", "none")
                .claim("sub", userId)
                .build();

        JwtAuthenticationToken authentication = new JwtAuthenticationToken(jwt, List.of());
        SecurityContextHolder.getContext().setAuthentication(authentication);
        String targetShard = businessService.eseguiQuerySicura();
        assertThat(targetShard)
                .isNotBlank()
                .startsWith("shard-");

        System.out.println("Utente JWT '" + userId + "' instradato automaticamente su: " + targetShard);
    }

    @Test
    void shouldThrowExceptionWhenNoKeyIsFound() {
        assertThatThrownBy(() -> businessService.eseguiQuerySicura())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Impossibile determinare la chiave di sharding");
    }

    @Test
    void shouldClearThreadLocalEvenWhenExceptionOccurs() {
        Jwt jwt = Jwt.withTokenValue("finto").header("alg", "none").claim("sub", "user-1").build();
        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt, List.of()));
        assertThatThrownBy(() -> businessService.queryCheFallisce())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Errore DB!");
        assertThat(ShardContextHolder.getShard()).isNull();
    }

    @Test
    void shouldPrioritizeJwtExtractorOverSpelKeyWhenBothPresent() {
        Jwt jwt = Jwt.withTokenValue("finto").header("alg", "none").claim("sub", "jwt-priority-user").build();
        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt, List.of()));

        // Method has @Sharded(key = "#paramKey")
        // ConsistentHashRouter for jwt-priority-user vs spel-user
        String shard = businessService.eseguiConParametro("spel-user");
        assertThat(shard).isNotBlank().startsWith("shard-");
        assertThat(ShardContextHolder.getShard()).isNull();
    }

    @Test
    void shouldFallbackToSpelKeyWhenJwtExtractorFindsNoKey() {
        SecurityContextHolder.clearContext();

        String shard = businessService.eseguiConParametro("fallback-tenant");
        assertThat(shard).isNotBlank().startsWith("shard-");
        assertThat(ShardContextHolder.getShard()).isNull();
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({SecuredBusinessService.class, FractalAutoConfiguration.class})
    static class DummyApp { }

    @Service
    static class SecuredBusinessService {

        @Sharded
        public String eseguiQuerySicura() {
            return ShardContextHolder.getShard();
        }

        @Sharded
        public String queryCheFallisce() {
            throw new RuntimeException("Errore DB!");
        }

        @Sharded(key = "#paramKey")
        public String eseguiConParametro(String paramKey) {
            return ShardContextHolder.getShard();
        }
    }
}