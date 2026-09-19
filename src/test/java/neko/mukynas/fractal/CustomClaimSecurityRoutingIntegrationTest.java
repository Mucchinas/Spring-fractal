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
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "fractal.sharding.jwt.claim-name=organization_id",
        "fractal.sharding.primary.jdbcUrl=jdbc:h2:mem:primarydb_custom",
        "fractal.sharding.primary.username=sa",
        "fractal.sharding.primary.password=",
        "fractal.sharding.shards.shard-1.jdbcUrl=jdbc:h2:mem:shard1db_custom",
        "fractal.sharding.shards.shard-1.username=sa",
        "fractal.sharding.shards.shard-1.password=",
        "fractal.sharding.shards.shard-2.jdbcUrl=jdbc:h2:mem:shard2db_custom",
        "fractal.sharding.shards.shard-2.username=sa",
        "fractal.sharding.shards.shard-2.password="
})
class CustomClaimSecurityRoutingIntegrationTest {

    @Autowired
    private SecuredTenantService tenantService;

    @AfterEach
    void tearDown() {
        ShardContextHolder.clear();
        SecurityContextHolder.clearContext();
    }

    @Test
    void shouldRouteUsingCustomConfiguredJwtClaim() {
        // Arrange: JWT with sub = user-123, but organization_id = acme-corp
        Jwt jwt = Jwt.withTokenValue("jwt-with-custom-claim")
                .header("alg", "none")
                .claim("sub", "user-123")
                .claim("organization_id", "acme-corp")
                .build();

        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt, List.of()));

        // Act
        String targetShard = tenantService.executeAction();

        // Assert
        assertThat(targetShard).isNotBlank().startsWith("shard-");
    }

    @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
    @Import({SecuredTenantService.class, FractalAutoConfiguration.class})
    static class CustomClaimApp {}

    @Service
    static class SecuredTenantService {
        @Sharded
        public String executeAction() {
            return ShardContextHolder.getShard();
        }
    }
}
