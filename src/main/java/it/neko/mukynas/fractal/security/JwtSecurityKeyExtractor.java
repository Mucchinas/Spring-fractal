package it.neko.mukynas.fractal.security;

import it.neko.mukynas.fractal.core.ShardingKeyExtractor;
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
        // Leggiamo il contesto di sicurezza del thread corrente
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication != null && authentication.isAuthenticated()) {

            // Se l'autenticazione è un JWT (Standard per Resource Server OAuth2)
            if (authentication instanceof JwtAuthenticationToken jwtAuthToken) {
                if ("sub".equalsIgnoreCase(claimName)) {
                    return jwtAuthToken.getToken().getSubject();
                }
                Object claimValue = jwtAuthToken.getToken().getClaims().get(claimName);
                return claimValue != null ? String.valueOf(claimValue) : null;
            }

            // Estensione futura: se volessimo supportare sessioni classiche
            // else if (authentication.getPrincipal() instanceof UserDetails) { ... }
        }

        return null; // Contesto assente o non supportato
    }
}