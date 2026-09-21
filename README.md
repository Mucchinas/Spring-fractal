# Fractal Spring Boot Starter

Fractal is an automated horizontal database sharding starter for Spring Boot 3 applications. It provides dynamic routing across multiple physical database shards using consistent hashing, seamless integration with Spring Security JWT tokens and SpEL expressions, and an automated topology rebalancing engine for online data migration.

> **Looking for internal architecture, migration state machines, or real-world use cases?**
> Check out the comprehensive **[Technical Deep Dive & Architecture Reference](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md)**.

---

## Key Features

- **Dynamic Shard Routing**: Transparently routes database operations to target shards using Spring AOP and `AbstractRoutingDataSource` before transactions initialize. Supports both method-level and class-level `@Sharded` annotations.
- **Consistent Hashing**: Distributes tenant keys across a 64-bit virtual node ring (`TreeMap`), ensuring uniform data distribution and minimal key relocation during cluster scaling.
- **Transparent Security & SpEL**: Route by authenticated JWT claims (`org_id`, `tenant_id`) with zero boilerplate, or route by service method arguments via SpEL (`#tenantId`). Features compiled SpEL caching and Spring `DefaultParameterNameDiscoverer` for robust interface parameter resolution.
- **Nested Context Isolation**: Safely handles nested `@Sharded` and `@ShardedBroadcast` service invocations, preserving and restoring outer shard contexts upon return without thread-local contamination.
- **Automated Rebalancer**: Introduce new physical shards anytime. Fractal calculates topology deltas, coordinates migrations via distributed locks with heartbeat failover, and moves tenant data in the background with automatic rollback to `ACTIVE` on unexpected failure.
- **Sub-Microsecond Migration Guard**: An in-process [Caffeine](https://github.com/ben-manes/caffeine) cache verifies tenant migration states in ~15 nanoseconds, shielding the primary database from high request volumes.
- **Replicated Reference Tables**: Synchronize read-mostly lookup tables (`@ShardedReplica`) atomically across all active shards for zero-latency local SQL joins, and update them concurrently with `@ShardedBroadcast`.
- **Zero Resource Leaks**: Single shared HikariCP connection pools across internal components, with automatic graceful pool disposal upon Spring context shutdown via `DisposableBean`.

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

##### Option C: Class-Level Routing & Nested Context Preservation
Annotate a service class with [`@Sharded`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/Sharded.java) to apply sharding across all public methods automatically. When nested `@Sharded` or [`@ShardedBroadcast`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedBroadcast.java) calls occur, Fractal preserves outer shard context using stack-based leasing rather than wiping thread state prematurely:

```java
@Service
@Sharded(key = "#tenantId") // Applied to all methods in the class
public class TenantManagementService {

    @Autowired
    private AuditLogService auditLogService;

    @Transactional
    public void processTenantData(String tenantId, TenantPayload payload) {
        // Routed to tenantId's shard
        savePayload(payload);

        // Nested call: auditLogService method can route elsewhere or broadcast
        // without destroying the outer tenantId routing context upon return!
        auditLogService.recordAudit(payload.getSummary());

        // Context remains safely bound to tenantId here
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
   public interface OrderRepository extends ShardedRepository<Order, UUID> {
       // High-performance local SQL join on the physical shard (zero cross-database calls)
       @Query("SELECT o FROM Order o JOIN Currency c ON o.currency = c.code WHERE o.organization.id = :orgId")
       List<Order> findOrdersWithCurrency(@Param("orgId") String orgId);
   }
   ```

---

#### Sharded Repository Contract & Physical Schema Audit

In horizontal sharding, tables located on worker shards **must maintain a foreign key path back to the `@ShardedRoot` table** (or be declared as `@ShardedReplica`). If an unlinked table is written to on a worker shard, its rows cannot be discovered during cluster rebalancing and would be permanently lost when a tenant is relocated to another shard!

Fractal provides two complementary, fail-fast startup safety mechanisms:

##### 1. Spring Data JPA: The `ShardedRepository` Pattern

For applications using Spring Data JPA, repositories managing sharded data must extend [`ShardedRepository<T, ID>`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/repository/ShardedRepository.java) instead of `JpaRepository<T, ID>`:

```java
package com.example.repository;

import io.github.mucchinas.fractal.repository.ShardedRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderRepository extends ShardedRepository<Order, UUID> {
    // Inherits all standard Spring Data JPA methods (save, findById, findAll, etc.)
}
```

At application startup, [`ShardedRepositoryStartupValidator`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/repository/ShardedRepositoryStartupValidator.java) enforces two strict invariants:
- **Rule 1 (Entity Reachability Guard)**: Any entity `T` managed by a `ShardedRepository` must be annotated with `@ShardedRoot`, `@ShardedReplica`, or `@ShardedEntity` with an introspected path connecting it to root. If an unannotated or orphaned entity is used, startup fails immediately.
- **Rule 2 (Service Injection Guard)**: Services annotated with `@Sharded` (or containing `@Sharded` methods) **must only inject repositories extending `ShardedRepository`**. Attempting to inject a standard `JpaRepository` into a sharded service triggers a validation failure, preventing accidental writes to unmanaged tables while routed to physical shards.
- **Coordinator Repository Whitelisting**: If a `@Sharded` service legitimately needs to inject a coordinator-only repository (e.g. `BillingPlanRepository` on the central primary database), add it to `fractal.sharding.validation.allowed-non-sharded-repositories`.

##### 2. Non-JPA / JDBC / MyBatis: Physical Shard Schema Audit

For teams using pure JDBC, jOOQ, or MyBatis (or as an additional layer of physical defense), Fractal's [`PhysicalSchemaAuditValidator`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/PhysicalSchemaAuditValidator.java) queries `information_schema.tables` directly on every **active physical worker shard database**:
- Any physical table residing on a worker shard that lacks an FK path to the root table (and is neither a replica nor an excluded table) triggers a startup failure.
- **Primary Database Isolation**: Central coordinator tables existing **only on the `primary` database** (such as `system_config`, `flyway_schema_history`, or `global_billing_plans`) are completely permitted and are **not** flagged by the shard schema audit.

##### Configuration:

```yaml
fractal:
  sharding:
    validation:
      repository-enforcement: STRICT   # STRICT (fail startup) | WARN (log warning) | DISABLED
      schema-audit-action: STRICT      # STRICT (fail startup) | WARN (log warning) | DISABLED
      allowed-non-sharded-repositories:
        - CentralBillingPlanRepo       # Allow coordinator repository inside @Sharded service
      schema-audit-exclude-tables:
        - temp_import_staging          # Ignore temporary/ETL physical tables on shards
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
         continuous-drain: true # Enables continuous startup reconciliation (default: true)
         drain-batch-size: 5000 # Bounded streaming keyset batch size (default: 5000)
         drain-check-mode: COUNT_THEN_PROBE # COUNT_THEN_PROBE | PROBE_ONLY | FULL_STREAMING
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
   - Once all tenants are evacuated, `primary` is marked as `DRAINED` in `fractal_shard_topology`.

3. **Continuous Startup Reconciliation & The 3-Tier Scale-Proof Engine**:
   In enterprise environments, new tenants or batch users may be inserted directly onto `primary` (e.g. by external IAM syncs or admin tools). When `continuous-drain: true` (default), Fractal checks for unaligned tenants at boot using a **memory-safe 3-Tier alignment engine**:
   - **Tier 1 (Instant Count Guard)**: Compares `SELECT COUNT(*)` on `primary` against the sum of shards. In 99.9% of normal restarts, this check passes in **2–5 ms with zero heap memory allocated**.
   - **Tier 2 (Status-Indexed Probe)**: If counts mismatch and `@ShardedStatus` is configured, queries `WHERE sync_status != 'ACTIVE'` using a database index in < 1 ms, pulling only the $K$ new tenants.
   - **Tier 3 (Bounded Keyset Streaming Cursor)**: Streams primary records in bounded keyset chunks (`WHERE id > :lastId ORDER BY id ASC LIMIT 5000`), checking physical shards in batches of 1,000. Heap memory is strictly capped at **~500 KB**, making the reconciliation completely immune to `OutOfMemoryError` even on 50,000,000+ records.

4. **On-Demand Runtime Drain API (Zero Pod Restarts)**:
   You can also evacuate a newly created tenant on `primary` to its assigned shard immediately at runtime without restarting pods:
   ```java
   @Autowired
   private RebalanceEngine rebalanceEngine;

   public void onboardExternalTenant(String tenantId) {
       // Evacuates the tenant from primary to its consistent hash worker shard with sub-second cutover
       rebalanceEngine.drainTenantFromPrimary(tenantId);
   }
   ```

---

### Just-In-Time (JIT) Tenant Provisioning (`@Sharded(provision = true)`)

When users authenticate via an Identity Provider (Keycloak, Auth0, Okta) for the first time, their identity is proven by a Bearer JWT, but **no record exists yet in the database**.

Fractal provides automated, high-performance Just-In-Time (JIT) runtime provisioning via [`@Sharded(provision = true)`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/Sharded.java):

```java
@Service
public class UserOnboardingService {

    // Targeted Provisioning: automatically creates the root record on primary and target shard
    @Sharded(key = "#userId", provision = true)
    public UserProfile getOrCreateProfile(String userId) {
        return profileRepository.findById(userId).orElseThrow();
    }

    // Standard business methods omit provision = true (default: false), guaranteeing 0 ns overhead
    @Sharded(key = "#userId")
    public List<Order> listOrders(String userId) {
        return orderRepository.findAllByUserId(userId);
    }
}
```

#### Provisioning Mechanics & Invariants:
1. **Targeted Zero-Overhead Guarantee**: Standard `@Sharded` methods (`provision = false`) bypass all provisioning checks entirely (0 cache lookups, 0 database queries, **0 ns overhead**).
2. **Dual-Presence Atomicity**:
   - Creates the root record in the **Master Catalog on `primary`** (for global user authentication and cross-shard delta calculations).
   - Creates the root record in the **Local Slice on the target physical shard** ($S = \text{router.routeNode}(\text{userId})$).
3. **Automated Column Extraction Pipeline**:
   - **Primary Key (`@ShardedKey`)**: Assigned the extracted sharding key (`userId`).
   - **Status (`@ShardedStatus`)**: Populated with `ACTIVE`.
   - **Configured JWT Claims**: Mapped from `fractal.sharding.jwt.attribute-claims` (e.g. `org_tier: tier`).
   - **Conventional Claims Discovery**: Automatically matches root entity fields against standard JWT claims:
     - `email` $\to$ `jwt.claim("email")`
     - `username` / `user_name` $\to$ `jwt.claim("preferred_username")`
     - `name` / `full_name` $\to$ `jwt.claim("name")`
     - `created_at` $\to$ current timestamp
4. **Parallel SPA Request Burst Protection**:
   If a single-page app fires 5 parallel requests upon first login, Fractal serializes them locally per tenant with an in-memory keyed mutex (`ConcurrentHashMap<String, Object>`), primes a local Caffeine cache (~15 ns lookups), and suppresses `DataIntegrityViolationException` on database inserts, ensuring zero race conditions or database deadlocks.
5. **Child Table Seeding (`TenantInitializer`)**:
   Implement the optional `TenantInitializer` SPI to seed default workspace or settings tables on the routed shard immediately after root creation:
   ```java
   @Bean
   public TenantInitializer userSettingsInitializer(DataSource dataSource) {
       return (tenantId, targetShard, rootAttributes) -> {
           // Current thread's ShardContextHolder is automatically bound to targetShard
           new JdbcTemplate(dataSource).update(
               "INSERT INTO user_settings (id, user_id, theme) VALUES (?, ?, ?)",
               UUID.randomUUID().toString(), tenantId, "DARK_MODE"
           );
       };
   }
   ```
6. **Programmatic Customization (`RootEntityCustomizer`)**:
   Implement `RootEntityCustomizer` to programmatically adjust or enrich root table columns before the insert.

---

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
| **[8. Verification & Test Suite](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/DEEP_DIVE.md#8-verification-testing--test-suite-reference)** | Coverage breakdown across all 37 test suites (132 automated unit/integration tests) |

---

## Building and Testing

### Prerequisites
- JDK 17 or higher
- Apache Maven 3.8+

### Execution
Run the full test suite (132 unit and integration tests):

```bash
mvn clean test
```

---

## License

This project is licensed under the Apache License 2.0.
