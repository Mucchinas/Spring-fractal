# Fractal Spring Boot Starter

Fractal is an automated horizontal database sharding starter for Spring Boot 3 applications. It provides dynamic routing across multiple physical database shards using consistent hashing, seamless integration with Spring Security JWT tokens and SpEL expressions, and an automated topology rebalancing engine for online data migration.

> **Looking for internal architecture, migration state machines, or real-world use cases?**
> Check out the comprehensive **[Technical Deep Dive & Architecture Reference](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md)**.

---

## Key Features

- **Dynamic Shard Routing**: Transparently routes database operations to target shards using Spring AOP and `AbstractRoutingDataSource` before transactions initialize.
- **Consistent Hashing**: Distributes tenant keys across a 64-bit virtual node ring (`TreeMap`), ensuring uniform data distribution and minimal key relocation during cluster scaling.
- **Transparent Security & SpEL**: Route by authenticated JWT claims (`org_id`, `tenant_id`) with zero boilerplate, or route by service method arguments via SpEL (`#tenantId`).
- **Automated Rebalancer**: Introduce new physical shards anytime. Fractal calculates topology deltas, coordinates migrations via distributed locks, and moves tenant data in the background.
- **Sub-Microsecond Migration Guard**: An in-process [Caffeine](https://github.com/ben-manes/caffeine) cache verifies tenant migration states in ~15 nanoseconds, shielding the primary database from high request volumes.
- **Replicated Reference Tables**: Synchronize read-mostly lookup tables (`@ShardedReplica`) across all shards for zero-latency local SQL joins, and update them concurrently with `@ShardedBroadcast`.

---

## Quickstart Guide

### 1. Installation

Add the starter dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.mucchinas</groupId>
    <artifactId>fractal-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

Enable parameter name preservation in `maven-compiler-plugin` so SpEL can resolve method argument names:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <version>3.13.0</version>
    <configuration>
        <parameters>true</parameters>
    </configuration>
</plugin>
```

---

### 2. Configuration

Configure your primary coordinator database and physical shard data sources in `application.yml`:

```yaml
fractal:
  sharding:
    enabled: true
    virtual-nodes: 150
    jwt:
      claim-name: org_id          # Optional: claim extracted from JWT (default: 'sub')
    primary:
      jdbc-url: jdbc:postgresql://localhost:5432/primary_db
      username: app_user
      password: ${DB_PASSWORD}
      initialize-schema: true     # Auto-create coordination tables on primary DB
    shards:
      shard-1:
        jdbc-url: jdbc:postgresql://localhost:5433/shard_1
        username: app_user
        password: ${DB_PASSWORD}
      shard-2:
        jdbc-url: jdbc:postgresql://localhost:5434/shard_2
        username: app_user
        password: ${DB_PASSWORD}
    rebalancer:
      enabled: true               # Enable background topology rebalancer
      lock-timeout: 15m           # Lock takeover TTL for crash recovery
      lock-refresh-interval: 1m   # Heartbeat interval to renew lock
```

---

### 3. Annotation Usage

#### Service Routing with `@Sharded`

##### Option A: Method Parameter Routing via SpEL
Annotate service methods with [`@Sharded(key = "...")`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/Sharded.java) to route requests based on method arguments:

```java
package com.example.service;

import io.github.mucchinas.fractal.annotation.Sharded;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    @Sharded(key = "#tenantId")
    @Transactional
    public Order createOrder(String tenantId, OrderRequest request) {
        // Queries execute against the physical shard mapped to tenantId
        return orderRepository.save(new Order(tenantId, request));
    }

    @Sharded(key = "#request.companyId")
    @Transactional(readOnly = true)
    public List<Order> getOrders(OrderQueryRequest request) {
        return orderRepository.findAllByCompany(request.getCompanyId());
    }
}
```

##### Option B: Transparent Routing via Spring Security JWT
When `spring-boot-starter-oauth2-resource-server` is present, Fractal automatically extracts the configured claim (e.g. `org_id`) from `SecurityContextHolder`:

```java
@Service
public class UserProfileService {

    // Transparently extracts the sharding key from the active JWT token
    @Sharded
    @Transactional(readOnly = true)
    public UserProfile getCurrentUserProfile() {
        return profileRepository.findCurrent();
    }
}
```

---

#### Domain Entity Modeling (Zero-Config Rebalancing)

Annotate your domain entities with [`@ShardedRoot`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedRoot.java) (for root partition entities), [`@ShardedEntity`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedEntity.java) (for child/dependent tables), [`@ShardedKey`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedKey.java), and [`@ShardedStatus`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedStatus.java). Fractal automatically infers the root table, partition column, status column, and foreign key dependency graph without requiring any YAML table configuration!

```java
// 1. Root Entity: Defines cluster partition key and migration status
@Entity
@Table(name = "organizations")
@ShardedRoot
public class Organization {
    @Id
    @ShardedKey
    private String id;

    private String name;

    @ShardedStatus
    @Column(name = "sync_status")
    private String syncStatus; // Automatically set to MIGRATING / ACTIVE during rebalances
}

// 2. Child Entity: References root entity via JPA relationship
@Entity
@Table(name = "projects")
@ShardedEntity
public class Project {
    @Id
    private UUID id;

    @ShardedKey
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "org_id")
    private Organization organization;
}

// 3. Leaf Entity: References parent via explicit scalar foreign key
@Entity
@Table(name = "tasks")
@ShardedEntity
public class Task {
    @Id
    private UUID id;

    @ShardedKey(targetEntity = Project.class, column = "project_id")
    private UUID projectId;
}
```

---

#### Replicated Reference Tables (`@ShardedReplica` & `@ShardedBroadcast`)

For global reference data (e.g. currencies, tax rates) that sharded queries must join against locally:

1. **Mark Entity as Replicated**:
   ```java
   @Entity
   @Table(name = "currencies")
   @ShardedReplica
   public class Currency {
       @Id private String code;
       private BigDecimal exchangeRate;
   }
   ```
   Fractal's [`ReplicaTableSynchronizer`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/ReplicaTableSynchronizer.java) automatically replicates reference tables from the primary coordinator database to every physical shard at application startup and whenever new shards join.

2. **Execute Multi-Shard Broadcast Writes**:
   ```java
   @Service
   public class CurrencyAdminService {
       @Autowired private CurrencyRepository currencyRepository;

       // Writes concurrently to primary and all physical shards in parallel
       @ShardedBroadcast
       @Transactional
       public void updateRate(String code, BigDecimal newRate) {
           currencyRepository.updateRate(code, newRate);
       }
   }
   ```

3. **Execute Local Shard Joins**:
   ```java
   @Repository
   public interface OrderRepository extends JpaRepository<Order, UUID> {
       // High-performance local SQL join on the physical shard (zero cross-database calls)
       @Query("SELECT o FROM Order o JOIN Currency c ON o.currency = c.code WHERE o.organization.id = :orgId")
       List<Order> findOrdersWithCurrency(@Param("orgId") String orgId);
   }
   ```

---

## Handling In-Flight Migrations

When a tenant is actively migrating to a new shard, Fractal throws a retryable [`TenantMigratingException`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/exception/TenantMigratingException.java) to prevent split-brain writes.

Handle this exception using a Spring `@RestControllerAdvice` to prompt clients or API gateways to retry:

```java
package com.example.web;

import io.github.mucchinas.fractal.exception.TenantMigratingException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ShardingExceptionHandler {

    @ExceptionHandler(TenantMigratingException.class)
    public ResponseEntity<String> handleTenantMigrating(TenantMigratingException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "5")
                .body("Tenant " + ex.getTenantId() + " is currently migrating shards. Please retry shortly.");
    }
}
```

---

## In-Depth Documentation

For advanced topics, architectural diagrams, and enterprise deployment scenarios, see [DEEP_DIVE.md](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md):

| Document Section | Topics Covered |
| :--- | :--- |
| **[1. Architectural Overview & Mechanics](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md#1-architectural-overview--request-interception-mechanics)** | Request lifecycle, AOP precedence (`@Order(1)` vs `@Transactional`), `ThreadLocal` context leasing |
| **[2. Core Components Deep Dive](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md#2-core-components-deep-dive)** | Consistent hash virtual node ring, HikariCP pools, key extractor fallback pipeline |
| **[3. Automated Rebalancer & Migration](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md#3-automated-rebalancer--migration-subsystem)** | Distributed locking & heartbeat, Caffeine cache, Kahn's topological sort, 2-phase crash recovery |
| **[4. Use Cases & Configurations](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md#4-use-cases--reference-configurations)** | 5 Architectural profiles (B2B SaaS, FinTech SpEL, Legacy `shard-all`, Reference tables, Event workers) |
| **[5. Configuration Reference](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md#5-configuration-property-specifications)** | Exhaustive property matrix and fully documented `application.yml` template |
| **[6. Advanced Usage & Patterns](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md#6-advanced-usage--integration-patterns)** | Custom `ShardingKeyExtractor`, JWT claim types, HTTP header extraction |
| **[7. Pitfalls & Architecture Solutions](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md#7-technical-considerations-pitfalls--solutions)** | Schema management, multi-datasource joins, the Transaction Aggregation Problem & `REQUIRES_NEW` dangers |
| **[8. Verification & Test Suite](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md#8-verification-testing--test-suite-reference)** | Coverage breakdown across all 16 test suites (59 automated unit/integration tests) |

---

## Building and Testing

### Prerequisites
- JDK 17 or higher
- Apache Maven 3.8+

### Execution
Run the full test suite (59 unit and integration tests):

```bash
mvn clean test
```

---

## License

This project is licensed under the Apache License 2.0.
