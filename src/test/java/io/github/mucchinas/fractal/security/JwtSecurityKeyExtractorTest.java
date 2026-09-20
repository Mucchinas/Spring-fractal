package io.github.mucchinas.fractal.security;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JwtSecurityKeyExtractorTest {

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void shouldExtractSubjectByDefault() {
        JwtSecurityKeyExtractor extractor = new JwtSecurityKeyExtractor();

        Jwt jwt = Jwt.withTokenValue("mock-token")
                .header("alg", "none")
                .claim("sub", "user-123")
                .build();
        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt, List.of()));

        assertThat(extractor.extractKey()).isEqualTo("user-123");
    }

    @Test
    void shouldExtractCustomClaimWhenConfigured() {
        JwtSecurityKeyExtractor extractor = new JwtSecurityKeyExtractor("tenant_id");

        Jwt jwt = Jwt.withTokenValue("mock-token")
                .header("alg", "none")
                .claim("sub", "user-123")
                .claim("tenant_id", "org-456")
                .build();
        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt, List.of()));

        assertThat(extractor.extractKey()).isEqualTo("org-456");
    }

    @Test
    void shouldHandleNumericClaimValuesGracefully() {
        JwtSecurityKeyExtractor extractor = new JwtSecurityKeyExtractor("account_number");

        Jwt jwt = Jwt.withTokenValue("mock-token")
                .header("alg", "none")
                .claim("sub", "user-123")
                .claim("account_number", 987654)
                .build();
        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt, List.of()));

        assertThat(extractor.extractKey()).isEqualTo("987654");
    }

    @Test
    void shouldReturnNullWhenCustomClaimIsMissing() {
        JwtSecurityKeyExtractor extractor = new JwtSecurityKeyExtractor("tenant_id");

        Jwt jwt = Jwt.withTokenValue("mock-token")
                .header("alg", "none")
                .claim("sub", "user-123")
                .build();
        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt, List.of()));

        assertThat(extractor.extractKey()).isNull();
    }

    @Test
    void shouldReturnNullWhenUnauthenticated() {
        JwtSecurityKeyExtractor extractor = new JwtSecurityKeyExtractor();

        Jwt jwt = Jwt.withTokenValue("mock-token")
                .header("alg", "none")
                .claim("sub", "user-123")
                .build();
        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt));

        assertThat(extractor.extractKey()).isNull();
    }
}
