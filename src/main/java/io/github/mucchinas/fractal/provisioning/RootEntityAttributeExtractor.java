package io.github.mucchinas.fractal.provisioning;

import io.github.mucchinas.fractal.config.FractalProperties;
import io.github.mucchinas.fractal.rebalance.EntityMetadataResult;
import io.github.mucchinas.fractal.rebalance.EntityTableMetadataResolver;
import jakarta.persistence.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

public class RootEntityAttributeExtractor {

    private static final Logger log = LoggerFactory.getLogger(RootEntityAttributeExtractor.class);

    private final FractalProperties properties;
    private final EntityTableMetadataResolver entityTableMetadataResolver;

    public RootEntityAttributeExtractor(FractalProperties properties, EntityTableMetadataResolver entityTableMetadataResolver) {
        this.properties = properties;
        this.entityTableMetadataResolver = entityTableMetadataResolver;
    }

    public Map<String, Object> extractAttributes(String tenantId, Authentication auth, Map<String, Object> customAttributes) {
        ensureMetadataResolved();

        Map<String, Object> columns = new LinkedHashMap<>();

        String rootIdCol = properties.getRebalancer().getRootIdColumn();
        String statusCol = properties.getRebalancer().getStatusColumn();
        String activeVal = properties.getRebalancer().getActiveValue() != null
                ? properties.getRebalancer().getActiveValue()
                : "ACTIVE";

        if (rootIdCol != null) {
            columns.put(rootIdCol, tenantId);
        }
        if (statusCol != null && !statusCol.isBlank()) {
            columns.put(statusCol, activeVal);
        }

        Map<String, Object> claims = extractClaims(auth);

        // 1. Configured JWT attribute claim mappings from application.yml
        Map<String, String> configuredClaims = properties.getJwt().getAttributeClaims();
        if (configuredClaims != null && !configuredClaims.isEmpty()) {
            for (Map.Entry<String, String> entry : configuredClaims.entrySet()) {
                String claimName = entry.getKey();
                String colName = entry.getValue();
                if (claims.containsKey(claimName)) {
                    columns.put(colName, claims.get(claimName));
                }
            }
        }

        // 2. Conventional entity field mappings
        Class<?> rootClass = entityTableMetadataResolver != null ? entityTableMetadataResolver.getRootClass() : null;
        if (rootClass != null) {
            mapConventionalFields(rootClass, claims, columns, rootIdCol, statusCol);
        } else {
            mapStandardClaimConventions(claims, columns);
        }

        // 3. Custom attributes override / supplement
        if (customAttributes != null && !customAttributes.isEmpty()) {
            columns.putAll(customAttributes);
        }

        // Ensure root ID is always present
        if (rootIdCol != null) {
            columns.put(rootIdCol, tenantId);
        }

        return columns;
    }

    private void ensureMetadataResolved() {
        if (properties.getRebalancer().getRootTable() == null && entityTableMetadataResolver != null) {
            try {
                EntityMetadataResult res = entityTableMetadataResolver.resolve();
                if (res != null) {
                    if (properties.getRebalancer().getRootTable() == null) {
                        properties.getRebalancer().setRootTable(res.rootTable());
                    }
                    if (properties.getRebalancer().getRootIdColumn() == null) {
                        properties.getRebalancer().setRootIdColumn(res.rootIdColumn());
                    }
                    if (properties.getRebalancer().getStatusColumn() == null) {
                        properties.getRebalancer().setStatusColumn(res.statusColumn());
                    }
                    if (properties.getRebalancer().getActiveValue() == null) {
                        properties.getRebalancer().setActiveValue(res.activeValue());
                    }
                }
            } catch (Exception e) {
                log.debug("FRACTAL: Entity metadata resolution deferred: {}", e.getMessage());
            }
        }
    }

    public Map<String, Object> extractClaims(Authentication auth) {
        if (auth == null) {
            return Collections.emptyMap();
        }

        Map<String, Object> claims = new HashMap<>();

        // 1. Check for Spring Security OAuth2 / JWT (org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken)
        try {
            Method getTokenMethod = auth.getClass().getMethod("getToken");
            Object token = getTokenMethod.invoke(auth);
            if (token != null) {
                Method getClaimsMethod = token.getClass().getMethod("getClaims");
                Object tokenClaims = getClaimsMethod.invoke(token);
                if (tokenClaims instanceof Map<?, ?> map) {
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        if (entry.getKey() != null) {
                            claims.put(entry.getKey().toString(), entry.getValue());
                        }
                    }
                    return claims;
                }
            }
        } catch (Exception ignored) {}

        // 2. Check principal for getClaims() (e.g. Jwt, OidcUser)
        Object principal = auth.getPrincipal();
        if (principal != null) {
            try {
                Method getClaimsMethod = principal.getClass().getMethod("getClaims");
                Object res = getClaimsMethod.invoke(principal);
                if (res instanceof Map<?, ?> map) {
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        if (entry.getKey() != null) {
                            claims.put(entry.getKey().toString(), entry.getValue());
                        }
                    }
                    return claims;
                }
            } catch (Exception ignored) {}

            // 3. Check principal for getAttributes() (e.g. OAuth2User, DefaultOAuth2User)
            try {
                Method getAttrsMethod = principal.getClass().getMethod("getAttributes");
                Object res = getAttrsMethod.invoke(principal);
                if (res instanceof Map<?, ?> map) {
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        if (entry.getKey() != null) {
                            claims.put(entry.getKey().toString(), entry.getValue());
                        }
                    }
                    return claims;
                }
            } catch (Exception ignored) {}
        }

        // 4. Check auth.getDetails()
        if (auth.getDetails() instanceof Map<?, ?> detailsMap) {
            for (Map.Entry<?, ?> entry : detailsMap.entrySet()) {
                if (entry.getKey() != null) {
                    claims.put(entry.getKey().toString(), entry.getValue());
                }
            }
            if (!claims.isEmpty()) {
                return claims;
            }
        }

        // 5. Fallback: sub / preferred_username = auth.getName()
        if (auth.getName() != null) {
            claims.put("sub", auth.getName());
            claims.put("preferred_username", auth.getName());
        }

        return claims;
    }

    private void mapConventionalFields(Class<?> rootClass,
                                       Map<String, Object> claims,
                                       Map<String, Object> columns,
                                       String rootIdCol,
                                       String statusCol) {
        List<Field> allFields = new ArrayList<>();
        Class<?> curr = rootClass;
        while (curr != null && curr != Object.class) {
            allFields.addAll(Arrays.asList(curr.getDeclaredFields()));
            curr = curr.getSuperclass();
        }

        for (Field field : allFields) {
            String colName = field.getName();
            Column colAnn = field.getAnnotation(Column.class);
            if (colAnn != null && !colAnn.name().isBlank()) {
                colName = colAnn.name();
            }

            if (columns.containsKey(colName)
                    || (rootIdCol != null && rootIdCol.equalsIgnoreCase(colName))
                    || (statusCol != null && statusCol.equalsIgnoreCase(colName))) {
                continue;
            }

            String normalized = colName.toLowerCase().replace("_", "");
            String fieldNormalized = field.getName().toLowerCase().replace("_", "");

            if ("email".equals(normalized) || "email".equals(fieldNormalized)) {
                if (claims.containsKey("email")) {
                    columns.put(colName, claims.get("email"));
                }
            } else if ("username".equals(normalized) || "username".equals(fieldNormalized)
                    || "preferredusername".equals(normalized) || "preferredusername".equals(fieldNormalized)) {
                Object val = claims.get("preferred_username");
                if (val == null) val = claims.get("username");
                if (val != null) {
                    columns.put(colName, val);
                }
            } else if ("name".equals(normalized) || "fullname".equals(normalized)
                    || "name".equals(fieldNormalized) || "fullname".equals(fieldNormalized)) {
                Object val = claims.get("name");
                if (val == null && claims.containsKey("given_name")) {
                    String given = String.valueOf(claims.get("given_name"));
                    String family = claims.containsKey("family_name") ? String.valueOf(claims.get("family_name")) : "";
                    val = (given + " " + family).trim();
                }
                if (val != null) {
                    columns.put(colName, val);
                }
            } else if ("createdat".equals(normalized) || "createdat".equals(fieldNormalized)) {
                columns.put(colName, createTimestampValue(field.getType()));
            } else if ("updatedat".equals(normalized) || "updatedat".equals(fieldNormalized)) {
                columns.put(colName, createTimestampValue(field.getType()));
            } else if (claims.containsKey(colName)) {
                columns.put(colName, claims.get(colName));
            } else if (claims.containsKey(field.getName())) {
                columns.put(colName, claims.get(field.getName()));
            }
        }
    }

    private void mapStandardClaimConventions(Map<String, Object> claims, Map<String, Object> columns) {
        if (!columns.containsKey("email") && claims.containsKey("email")) {
            columns.put("email", claims.get("email"));
        }
        if (!columns.containsKey("username") && (claims.containsKey("preferred_username") || claims.containsKey("username"))) {
            Object val = claims.get("preferred_username") != null ? claims.get("preferred_username") : claims.get("username");
            columns.put("username", val);
        }
    }

    private Object createTimestampValue(Class<?> fieldType) {
        if (Instant.class.isAssignableFrom(fieldType)) {
            return Instant.now();
        }
        if (LocalDateTime.class.isAssignableFrom(fieldType)) {
            return LocalDateTime.now();
        }
        if (Date.class.isAssignableFrom(fieldType)) {
            return new Date();
        }
        return new Timestamp(System.currentTimeMillis());
    }
}
