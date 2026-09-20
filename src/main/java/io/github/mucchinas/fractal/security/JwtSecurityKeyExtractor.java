package io.github.mucchinas.fractal.security;

import io.github.mucchinas.fractal.core.ShardingKeyExtractor;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

public class JwtSecurityKeyExtractor implements ShardingKeyExtractor {

    private final String claimName;

    public JwtSecurityKeyExtractor() {
        this("sub");
    }

    public JwtSecurityKeyExtractor(String claimName) {
        this.claimName = (claimName != null && !claimName.isBlank()) ? claimName : "sub";
    }

    public String getClaimName() {
        return claimName;
    }

    @Override
    public String extractKey() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication != null && authentication.isAuthenticated()) {
            if (authentication instanceof JwtAuthenticationToken jwtAuthToken) {
                if ("sub".equalsIgnoreCase(claimName)) {
                    return jwtAuthToken.getToken().getSubject();
                }
                Object claimValue = jwtAuthToken.getToken().getClaims().get(claimName);
                return claimValue != null ? String.valueOf(claimValue) : null;
            }
        }

        return null;
    }
}