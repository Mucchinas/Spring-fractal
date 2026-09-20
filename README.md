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

// 2. Child Entity (JPA Object Reference): Target entity inferred from Organization field type
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

// 3. Leaf Entity (Scalar Foreign Key): Uses UUID instead of JPA entity reference
@Entity
@Table(name = "tasks")
@ShardedEntity
public class Task {
    @Id
    private UUID id;

    // Best practice for scalar IDs: specify targetEntity = Project.class.
    // The foreign key column in tasks ("project_id") is inferred from @Column(name) or field name.
    @Column(name = "project_id")
    @ShardedKey(targetEntity = Project.class)
    private UUID projectId;
}
```

##### Understanding the `@ShardedKey` Hop with Scalar IDs (UUID / Long)

In modern Domain-Driven Design (DDD) and high-throughput systems, developers often store scalar foreign keys (`UUID projectId`) rather than full JPA entity relationships (`@ManyToOne Project project`) to eliminate lazy-loading overhead, N+1 queries, and deep Hibernate proxy graphs.

When using scalar IDs, Java reflection only sees `java.util.UUID` without any entity association. The [`@ShardedKey`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedKey.java) annotation provides the missing link for Fractal to construct the migration dependency graph:

| `@ShardedKey` Attribute | Which Table it Refers To | Scalar UUID Usage | JPA `@ManyToOne` Usage | Default / Inference Behavior |
| :--- | :--- | :--- | :--- | :--- |
| `targetEntity` | **Parent Table** | **Mandatory** (`Project.class`) | Optional | Inferred from Java field type (e.g. `Organization.class`) |
| `column` | **This Entity's Table** (Child) | Optional | Optional | Inferred from `@Column(name)`, `@JoinColumn(name)`, or field name in `snake_case` (`project_id`) |
| `referencedColumn` | **Parent Table** | Optional | Optional | Inferred from parent's `@Id` primary key (or root partition key) |

##### How the Rebalancer Uses the Hop Graph (Synthesized SQL)

When moving an organization (`'org_123'`) from Shard A to Shard B, Fractal traverses the hop graph from [`@ShardedRoot`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedRoot.java) down to leaf entities and synthesizes exact migration queries:

```sql
-- Hop 0: Root Entity
SELECT * FROM organizations WHERE id = 'org_123';

-- Hop 1: Child Entity (via org_id foreign key)
SELECT * FROM projects WHERE org_id = 'org_123';

-- Hop 2: Leaf Entity (via scalar UUID projectId -> projects.id -> org_id)
SELECT tasks.* FROM tasks
JOIN projects ON tasks.project_id = projects.id
WHERE projects.org_id = 'org_123';
```

---

#### Mandatory Invariant & Best Practices: Aligning the Service Routing Key with `@ShardedRoot`

> [!IMPORTANT]
> **The Golden Rule of Fractal Sharding**:
> The value extracted at runtime by [`@Sharded`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/Sharded.java) (from JWT claims or SpEL arguments) and the value stored in the [`@ShardedKey`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedKey.java) column of the [`@ShardedRoot`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedRoot.java) table **must represent the exact same identifier and refer to the exact same value in the database**.
>
> The chosen sharding key **must be physically present as a column in the root table** (typically its `@Id` or unique partition key).

##### Why This Alignment is Mandatory (The Mechanics)

Fractal operates on two cooperating paths:
1. **Service Routing Path**: When a service method executes, [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) extracts the sharding key and hashes it to select the physical shard:
   $$\text{Target Shard} = \text{router.routeNode}(\text{extractedKey})$$
2. **Rebalance Migration Path**: When physical shards are added or scaled, [`MigrationDeltaCalculator`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/MigrationDeltaCalculator.java) queries the primary database root table to plan data movements:
   $$\text{Planned Shard} = \text{router.routeNode}(\text{rootTable}.\text{rootIdColumn})$$
   It then copies the root row and joins all child tables using the `@ShardedKey` hop graph starting from that root ID.

**The Fatal Failure Mode (Key Disconnect)**:
If your service extracts a user ID (`"usr_999"` from JWT `sub`), but your `@ShardedRoot` table is `Organization` where `@ShardedKey` is `organizations.id` (`"org_123"`):
- The service hashes `"usr_999"` and routes queries to **Shard 1**.
- But the rebalancer hashed `"org_123"` and placed that organization's data on **Shard 2**!
- Queries execute on Shard 1 looking for data that physically lives on Shard 2, resulting in missing records, foreign key violations, or empty query results.

##### Recommended Best Practice Patterns

###### Pattern 1: B2B Multi-Tenant SaaS (Partition by Tenant / Organization)
In multi-tenant SaaS, all operational data belongs to an enterprise tenant (organization, workspace, company).

- **Root Entity**:
  ```java
  @Entity
  @Table(name = "organizations")
  @ShardedRoot
  public class Organization {
      @Id
      @ShardedKey
      private String id; // Physical column: "id", value: "org_123"
      
      @ShardedStatus
      private String syncStatus;
  }
  ```
- **Key Extraction Strategy**:
  - *Via JWT Claim (Recommended)*: Set `fractal.sharding.jwt.claim-name: tenant_id` (or `org_id`). Ensure your identity provider (Keycloak, Auth0, Okta) embeds the tenant identifier in the access token:
    ```json
    { "sub": "usr_abc", "tenant_id": "org_123", "roles": ["ADMIN"] }
    ```
    Service methods simply use `@Sharded` with zero arguments.
  - *Via Method Parameter*: For background or internal batch jobs, pass the organization ID:
    ```java
    @Sharded(key = "#orgId")
    public void generateReport(String orgId) { ... }
    ```

###### Pattern 2: B2C Consumer Platforms (Partition by User / Account)
In consumer platforms (wallets, shopping carts, gaming), data belongs directly to individual end-users.

- **Root Entity**:
  ```java
  @Entity
  @Table(name = "users")
  @ShardedRoot
  public class User {
      @Id
      @ShardedKey
      private String id; // Physical column: "id", value: "usr_456"
      
      @ShardedStatus
      private String syncStatus;
  }
  ```
- **Key Extraction Strategy**:
  - *Via JWT Subject (Default)*: Keep the default configuration `fractal.sharding.jwt.claim-name: sub`. Standard OAuth2/OIDC tokens store the user's unique identifier in the `sub` claim:
    ```json
    { "sub": "usr_456", "email": "alice@example.com" }
    ```
    Service methods use `@Sharded` with zero arguments.
  - *Via Method Parameter*:
    ```java
    @Sharded(key = "#userId")
    public UserProfile updateProfile(String userId, ProfileDto dto) { ... }
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

## Online Shard Rebalancing & Dynamic Dual-Ring Routing

When physical shards are added or removed (e.g. scaling from 2 to 5 shards), Fractal automatically executes online rebalancing without application downtime.

### Zero-Overhead Normal Operations (Steady-State)

In steady-state (when no rebalance is in progress):
- Fractal evaluates a single cached boolean flag [`isRebalanceActive()`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TopologyManager.java#L94) backed by a 1-element Caffeine cache.
- The [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) takes the **fast path**: requests route directly via the consistent hash ring with **zero** per-tenant Caffeine cache lookups, **zero** database queries, and **zero** in-flight request tracking overhead.
- The per-tenant status cache remains unpopulated during normal operations, eliminating cache churn.

### Dynamic Dual-Ring Routing During Rebalancing

When a cluster rebalance starts:
1. **Cluster Lock & Global Status**: The rebalance leader acquires the distributed lock (`REBALANCE_LOCK`) and sets `isRebalanceActive = true` (cached across pods via Caffeine).
2. **Pending Migrations ($R_{old}$ Override)**: All tenants scheduled to move to new shards are registered with pending source overrides. While awaiting their individual turn, incoming requests for these tenants continue routing to their **old source shard**.
3. **Active Migration (Immediate Isolation)**: Tenants are migrated **one at a time**. When an individual tenant's turn starts:
   - Tenant is marked `MIGRATING`.
   - In-flight transactions drain.
   - Incoming requests throw [`TenantMigratingException`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/exception/TenantMigratingException.java) (HTTP 503 with `Retry-After: 5`) to prevent split-brain writes.
   - Relational data is copied topologically to the new shard and pruned from the old shard.
4. **Immediate Cutover ($R_{new}$)**: As soon as an individual tenant finishes migrating, its pending override is removed and it **immediately cuts over to the new shard ($R_{new}$)**. It does not wait for the rest of the batch to finish.
5. **Non-Migrating Bystanders**: Unaffected tenants whose consistent hash placement did not change continue executing normally without interruption.
6. **Return to Steady State**: Once all migrations finish and the distributed lock is released, `isRebalanceActive` reverts to `false`, clearing all overrides and returning the system to zero-overhead fast-path routing.

### Handling In-Flight Migrations in Web Layer

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

### Sharding an Already-Populated Database (Brownfield Onboarding)

When adopting Fractal into an existing application whose database already contains tables and data, you have two native adoption paths:

#### Option A: The 1-Shard Baseline Pattern (Zero Data Copying)
1. **Phase 1: Adopt with 1 Shard**:
   Declare your **existing database as `shard-1`** (and point `primary` to the same DB or coordinator):
   ```yaml
   fractal:
     sharding:
       primary:
         jdbc-url: jdbc:postgresql://db-main:5432/myapp
       shards:
         shard-1:
           jdbc-url: jdbc:postgresql://db-main:5432/myapp # Existing populated DB
   ```
   100% of routing keys map to `shard-1`. Zero data moves over the network, and the app runs immediately with zero downtime.
2. **Phase 2: Scale to Multiple Shards ($1 \to N$ Shards)**:
   When ready to scale, add `shard-2` and `shard-3` to `application.yml` with `rebalancer.enabled: true`. On next startup, Fractal automatically rebalances ~66% of tenants to the new shards, while ~33% remain untouched on `shard-1`.

#### Option B: The Primary Drain Pattern (`primary.drain: true`)
If you want to keep the existing database purely as a lightweight cluster coordinator and evacuate all sharded tables into brand-new, clean shard instances (`shard-1`, `shard-2`, etc.):

1. **Configure Shards and Set `drain: true` on Primary**:
   ```yaml
   fractal:
     sharding:
       primary:
         jdbc-url: jdbc:postgresql://db-monolith:5432/myapp # Existing database
         drain: true # Instructs Fractal to safely evacuate all sharded data
       shards:
         shard-1:
           jdbc-url: jdbc:postgresql://db-shard1:5432/myapp # New shard instance
         shard-2:
           jdbc-url: jdbc:postgresql://db-shard2:5432/myapp # New shard instance
       rebalancer:
         enabled: true
   ```
2. **Automated Zero-Downtime Evacuation**:
   - At startup, Fractal initializes the routing ring with `[shard-1, shard-2]`.
   - Dual-Ring Routing routes active queries for unmigrated tenants to `primary` via pending overrides.
   - For every tenant, Fractal copies the root record and child tables to their assigned shard, prunes child tables from `primary`, and immediately cuts over live traffic to the target shard.
   - The master tenant row in `rootTable` is preserved on `primary` (with status `ACTIVE`), keeping `primary`'s catalog intact.
   - Once all tenants are evacuated, `primary` is marked as `DRAINED` in `fractal_shard_topology`. Subsequent startups skip the drain automatically.

### Graceful Shard Decommissioning & Cluster Scale-Down ($M \to N$ Shards)

Fractal natively supports scaling down clusters (e.g. contracting from 3 shards to 2 shards, or retiring a maintenance node) without downtime:

1. **The Decommissioning Invariant**: In distributed architectures, a retiring node cannot be decommissioned by abruptly deleting its credentials from `application.yml`, because Fractal requires database connectivity to the retiring shard to read, copy, and prune tenant aggregates.
2. **Step 1: Mark for Decommissioning in YAML**:
   Retain the database credentials in `application.yml` and set `decommission: true` (or `status: DRAINING`):
   ```yaml
   fractal:
     sharding:
       shards:
         shard-1:
           jdbc-url: jdbc:postgresql://db-shard1:5432/myapp
           username: myapp_admin
           password: ${DB_PASS}
           decommission: true # Marks shard for automated draining
         shard-2:
           jdbc-url: jdbc:postgresql://db-shard2:5432/myapp
           username: myapp_admin
           password: ${DB_PASS}
         shard-3:
           jdbc-url: jdbc:postgresql://db-shard3:5432/myapp
           username: myapp_admin
           password: ${DB_PASS}
   ```
3. **Step 2: Automated Drain & Safe Dual-Ring Migration**:
   - At startup, Fractal initializes the runtime consistent hash ring with **active shards only** (`[shard-2, shard-3]`).
   - Dynamic Dual-Ring Routing registers pending source overrides for all tenants on `shard-1`, routing active queries to `shard-1` until each tenant's migration completes.
   - The [`RebalanceEngine`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/RebalanceEngine.java) iterates over tenants on `shard-1`, migrating them topologically to surviving shards.
   - Once all tenants are completely evacuated, Fractal automatically deletes `shard-1` from `fractal_shard_topology`.
4. **Step 3: Physical Teardown**:
   - The operator turns off the `shard-1` database instance.
   - The `shard-1` configuration block can now be safely removed from `application.yml` in subsequent deployments.

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
| **[8. Verification & Test Suite](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md#8-verification-testing--test-suite-reference)** | Coverage breakdown across all 20 test suites (97 automated unit/integration tests) |

---

## Building and Testing

### Prerequisites
- JDK 17 or higher
- Apache Maven 3.8+

### Execution
Run the full test suite (97 unit and integration tests):

```bash
mvn clean test
```

---

## License

This project is licensed under the Apache License 2.0.
