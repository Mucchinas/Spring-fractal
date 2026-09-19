# Fractal Spring Boot Starter

Fractal is an automated horizontal database sharding starter for Spring Boot 3 applications. It provides dynamic routing across multiple physical database shards using consistent hashing, seamless integration with Spring Security JWT tokens and SpEL expressions, and an automated topology rebalancing engine for data migration.

---

## Table of Contents

- [Architectural Overview](#architectural-overview)
- [Core Components](#core-components)
  - [Consistent Hash Router](#consistent-hash-router)
  - [Sharding Routing DataSource](#sharding-routing-datasource)
  - [Sharding Aspect and Interception Order](#sharding-aspect-and-interception-order)
  - [Key Extraction Pipeline](#key-extraction-pipeline)
- [Automated Rebalancer and Migration Subsystem](#automated-rebalancer-and-migration-subsystem)
  - [Topology Management, Distributed Locking, and Heartbeat](#topology-management-distributed-locking-and-heartbeat)
  - [Delta Calculation](#delta-calculation)
  - [Domain Entity Auto-Discovery (@ShardedEntity & @ShardedKey)](#domain-entity-auto-discovery-shardedentity--shardedkey)
  - [Database Catalog Dependency Resolution](#database-catalog-dependency-resolution)
  - [Rebalance Execution Lifecycle & Idempotent Crash Recovery](#rebalance-execution-lifecycle--idempotent-crash-recovery)
- [Configuration Reference](#configuration-reference)
  - [Property Specifications](#property-specifications)
  - [Configuration Example](#configuration-example)
- [Usage Guide](#usage-guide)
  - [Maven Dependency](#maven-dependency)
  - [Service-Level Annotation with SpEL](#service-level-annotation-with-spel)
  - [Transparent Routing via Spring Security JWT](#transparent-routing-via-spring-security-jwt)
  - [Handling Rebalance Migration Lock (TenantMigratingException)](#handling-rebalance-migration-lock-tenantmigratingexception)
  - [Implementing a Custom ShardingKeyExtractor](#implementing-a-custom-shardingkeyextractor)
- [Technical Considerations and Architecture Patterns](#technical-considerations-and-architecture-patterns)
  - [Schema Management Best Practices](#schema-management-best-practices)
  - [Joining Sharded and Non-Sharded Data](#joining-sharded-and-non-sharded-data)
  - [The Transaction Aggregation Problem and Propagation.REQUIRES_NEW Caveats](#the-transaction-aggregation-problem-and-propagationrequires_new-caveats)
- [Building and Testing](#building-and-testing)

---

## Architectural Overview

Fractal intercepts business method execution at the service layer to resolve a sharding key, hashes that key onto a virtual consistent hashing ring, and binds the selected shard identifier to the current execution thread. Spring's `AbstractRoutingDataSource` dynamically dispatches database connections to the designated shard before transactional connections are initialized.

```
+-----------------------------------------------------------------------+
|                            Client Request                             |
|          (HTTP REST Request / Scheduled Job / Event Consumer)         |
+-----------------------------------+-+---------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                 Spring AOP Proxy (@Sharded Interceptor)                |
|                                                                       |
|  1. Extract Sharding Key:                                             |
|     - Priority A: ShardingKeyExtractor chain (e.g. JWT claim)          |
|     - Priority B: SpEL expression evaluation on method arguments       |
|                                                                       |
|  2. Tenant Migration Guard:                                           |
|     - Check TopologyManager.isTenantMigrating(key)                    |
|     - Throws TenantMigratingException if migration is in flight       |
|                                                                       |
|  3. ConsistentHashRouter:                                             |
|     - Hash key with MD5 onto 64-bit virtual node ring                 |
|     - Resolve target shard name (e.g., "shard-1")                     |
|                                                                       |
|  4. ShardContextHolder:                                               |
|     - Bind shard name to ThreadLocal                                  |
+-----------------------------------+-+---------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                 Spring @Transactional Boundary                        |
|                                                                       |
|  - Calls DataSource.getConnection()                                   |
|  - ShardingRoutingDataSource queries ShardContextHolder.getShard()    |
|  - Obtains and binds physical connection from target HikariCP pool    |
+-----------------------------------+-+---------------------------------+
                                    |
          +-------------------------+-------------------------+
          |                         |                         |
          v                         v                         v
+-------------------+     +-------------------+     +-------------------+
|   Shard 1 (DB)    |     |   Shard 2 (DB)    |     |   Primary (DB)    |
| (Tenant Data A-M) |     | (Tenant Data N-Z) |     | (Topology & Meta) |
+-------------------+     +-------------------+     +-------------------+
```

---

## Core Components

### Consistent Hash Router

`ConsistentHashRouter` (`neko.mukynas.fractal.core.ConsistentHashRouter`) distributes keys uniformly across physical shards while minimizing data movement during cluster expansion.

- **Virtual Nodes**: Each physical shard is mapped to multiple positions on the ring (`virtualNodes`, default: `150`) using the naming convention `<shardName>-VN-<i>`.
- **Ring Structure**: Implemented via a `java.util.TreeMap<Long, String>`.
- **Hashing Function**: Computes an MD5 digest of the input key and maps the first 8 bytes into a 64-bit signed `Long`.
- **Lookup Complexity**: Binary search traversal via `TreeMap.tailMap(hash)`. If no higher key exists, it wraps around to `ring.firstKey()`.

### Sharding Routing DataSource

`ShardingRoutingDataSource` (`neko.mukynas.fractal.datasource.ShardingRoutingDataSource`) extends Spring JDBC's `AbstractRoutingDataSource`.

- Overrides `determineCurrentLookupKey()` to retrieve the active shard identifier from `ShardContextHolder.getShard()`.
- Pre-configures a target map containing individual `HikariDataSource` connection pools for each configured shard.
- Accepts a designated `primary` datasource as the default fallback target (`setDefaultTargetDataSource`).

### Sharding Aspect and Interception Order

`ShardingAspect` (`neko.mukynas.fractal.aop.ShardingAspect`) intercepts any method annotated with `@Sharded`.

```java
@Aspect
@Order(1)
public class ShardingAspect { /*...*/ }
```

The `@Order(1)` declaration is mandatory. In Spring, `@Transactional` aspects execute at default lowest precedence (`Ordered.LOWEST_PRECEDENCE`). By configuring `@Order(1)`, `ShardingAspect` guarantees that:
1. The target shard identifier is resolved and placed into `ShardContextHolder` before `DataSourceTransactionManager` attempts to open a database connection.
2. The context is cleared within a `finally` block immediately after the join point returns or throws an exception, preventing thread-local pollution in pooled worker threads.

### Key Extraction Pipeline

Key extraction follows a deterministic fallback hierarchy:

1. **Strategic Extraction**: Iterates over all registered `ShardingKeyExtractor` beans (ordered stream from `ObjectProvider<ShardingKeyExtractor>`). If any extractor returns a non-blank string, that value is selected.
2. **SpEL Evaluation**: If strategic extraction yields no result and `@Sharded(key = "...")` is specified, the expression is evaluated against the method arguments using Spring's `SpelExpressionParser` and `StandardEvaluationContext`.
3. **Guard Condition**: If neither strategy produces a key, an `IllegalStateException` is raised, halting transaction initialization.

---

## Automated Rebalancer and Migration Subsystem

Fractal includes a background data rebalancing mechanism designed to handle cluster growth (adding new physical shards) with minimal tenant interruption.

```
+------------------------------------------------------------------------------------+
|                         Cluster Startup & Rebalance Cycle                          |
+-----------------------------------------+------------------------------------------+
                                          |
                                          v
                    +--------------------------------------------+
                    |  CommandLineRunner (Startup Listener)      |
                    |  Check YAML shards vs DB topology table    |
                    +---------------------+----------------------+
                                          |
                        [Discrepancy Detected]
                                          |
                                          v
                    +--------------------------------------------+
                    |  TopologyManager: tryAcquireRebalanceLock  |
                    |  (Atomic INSERT into fractal_locks)        |
                    +---------------------+----------------------+
                                          |
                                          v
                    +--------------------------------------------+
                    |  MigrationDeltaCalculator: calculateDelta  |
                    |  oldRouter(dbShards) vs newRouter(yaml)    |
                    |  Identify tenant IDs where target changed  |
                    +---------------------+----------------------+
                                          |
                                          v
                    +--------------------------------------------+
                    |  TableDependencyResolver:                  |
                    |  Query information_schema Foreign Keys     |
                    |  Kahn's Topological Sort (Insert & Delete) |
                    +---------------------+----------------------+
                                          |
                                          v
                    +--------------------------------------------+
                    |  RebalanceEngine:                          |
                    |  1. UPDATE root SET status = 'MIGRATING'   |
                    |  2. Batch copy rows in topological order   |
                    |  3. Batch delete rows in reverse order     |
                    |  4. UPDATE root SET status = 'ACTIVE'      |
                    +---------------------+----------------------+
                                          |
                                          v
                    +--------------------------------------------+
                    |  TopologyManager: releaseRebalanceLock     |
                    |  Record new shards in fractal_shard_topology
                    +--------------------------------------------+
```

### Topology Management, Distributed Locking, and Heartbeat

`TopologyManager` (`neko.mukynas.fractal.rebalance.TopologyManager`) coordinates rebalancing across clustered application nodes:

- **`fractal_shard_topology`**: Persists the known active shards and their statuses.
- **`fractal_locks`**: Implements mutual exclusion across multiple application instances using primary key constraint semantics:
  ```sql
  CREATE TABLE IF NOT EXISTS fractal_locks (
      lock_name VARCHAR(255) PRIMARY KEY,
      locked_by VARCHAR(255) NOT NULL,
      locked_at TIMESTAMP NOT NULL
  );
  ```
  An initial atomic acquisition is attempted via insert:
  ```sql
  INSERT INTO fractal_locks (lock_name, locked_by, locked_at) VALUES ('REBALANCE_LOCK', ?, ?)
  ```
- **Lock TTL and Atomic Crash Takeover**:
  If an application instance crashes abruptly (such as from a power outage, SIGKILL, or OOMKilled event), `fractal_locks` retains the acquired row. To avoid permanent cluster deadlock without manual DBA intervention, a configurable timeout is enforced (`fractal.sharding.rebalancer.lock-timeout`, default `15m`). If the initial `INSERT` fails due to a primary key constraint, `TopologyManager` evaluates whether the existing lock has expired:
  ```sql
  UPDATE fractal_locks
  SET locked_by = ?, locked_at = ?
  WHERE lock_name = 'REBALANCE_LOCK' AND locked_at < ?
  ```
  where the cutoff threshold is `now - lockTimeout`. If rows are affected, the expired lock is atomically commandeered by the current node.
- **Periodic Heartbeat Renewal**:
  To protect long-running migrations (large datasets spanning many tables and rows) from premature lock expiration and hostile takeovers, `TopologyManager` runs a background daemon thread that periodically refreshes the lock timestamp at a configurable interval (`fractal.sharding.rebalancer.lock-refresh-interval`, default `1m`):
  ```sql
  UPDATE fractal_locks
  SET locked_at = CURRENT_TIMESTAMP
  WHERE lock_name = 'REBALANCE_LOCK' AND locked_by = ?
  ```
- **Graceful Shutdown Integration**:
  `TopologyManager` implements Spring's `DisposableBean` (`destroy()`). When an application context receives a shutdown signal (such as `SIGTERM` during container termination):
  1. The heartbeat daemon thread is cancelled and stopped cleanly.
  2. The `REBALANCE_LOCK` row is released immediately in `fractal_locks`.
  3. Spring's `fractalRebalanceExecutor` is configured with `setWaitForTasksToCompleteOnShutdown(true)` and `setAwaitTerminationSeconds(30)` to permit currently in-flight batch copies to finish before complete termination.

### Delta Calculation

`MigrationDeltaCalculator` (`neko.mukynas.fractal.rebalance.MigrationDeltaCalculator`) constructs two consistent hash rings simultaneously:
- **Old Topology**: Constructed from shards currently registered in `fractal_shard_topology`.
- **New Topology**: Constructed from the total shard definition declared in configuration.

It scans all entity identifiers in `rootTable` and filters records where `oldRouter.routeNode(id)` differs from `newRouter.routeNode(id)`, yielding an execution plan of `MigrationAction(id, sourceShard, targetShard)` records.

### Domain Entity Auto-Discovery (@ShardedEntity & @ShardedKey)

Fractal provides a declarative domain-driven discovery engine via `EntityTableMetadataResolver` (`neko.mukynas.fractal.rebalance.EntityTableMetadataResolver`), using `@ShardedEntity` and `@ShardedKey`:

1. **Root Partition Anchor**: Exactly one domain entity is marked with `@ShardedEntity(root = true)`. The key field on this entity (annotated with `@ShardedKey` or JPA `@Id`) serves as the cluster partition key (`rootTable` and `rootIdColumn`).
2. **Foreign Key Hopping**: Descendant entities annotated with `@ShardedEntity` declare a `@ShardedKey` on the field or method that hops back towards the root:
   - **Entity References**: When the field references another `@ShardedEntity` (e.g. `@ManyToOne Organization organization`), the target entity is inferred automatically.
   - **Scalar Foreign Keys**: For raw ID columns (e.g. `UUID projectId`), the target entity is specified explicitly via `@ShardedKey(targetEntity = Project.class, column = "project_id")`.
3. **Physical DB Foreign Key Independence**: Auto-discovery operates directly on Java domain models. This enables full topological migration planning even on high-throughput database clusters where physical database foreign key constraints are omitted for performance.
4. **Graph Validation on Startup**:
   - Ensures exactly one root entity exists.
   - Verifies acyclicity (cycle detection).
   - Validates that every descendant entity can reach the root entity via hops.
5. **Zero-Config Rebalancing**: If `@ShardedEntity` classes are present, `root-table`, `root-id-column`, and `sharded-tables` in `application.yml` are completely optional.

#### Domain Model Example:

```java
@Entity
@Table(name = "organizations")
@ShardedEntity(root = true)
public class Organization {
    @Id
    @ShardedKey
    private String id;
    private String name;
}

@Entity
@Table(name = "projects")
@ShardedEntity
public class Project {
    @Id
    private UUID id;

    // Hop 1: Inferred target entity from Organization type
    @ShardedKey
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "org_id")
    private Organization organization;
}

@Entity
@Table(name = "tasks")
@ShardedEntity
public class Task {
    @Id
    private UUID id;

    // Hop 2: Explicit scalar foreign key pointing to Project
    @ShardedKey(targetEntity = Project.class, column = "project_id")
    private UUID projectId;
}
```

### Database Catalog Dependency Resolution

When domain entity annotations are not used, `TableDependencyResolver` (`neko.mukynas.fractal.rebalance.TableDependencyResolver`) falls back to introspecting database foreign key constraints using standard ANSI `information_schema` views (`referential_constraints` and `key_column_usage`):

1. **Catalog Auto-Discovery**: If `sharded-tables` is omitted, Fractal recursively traces foreign key relationships starting from `rootTable`.
2. **Exclusion Filtering**: Tables can be excluded from discovery via `exclude-tables`.
3. **Foreign Key Hopping**: For tables without a direct root foreign key, the resolver generates relational `JOIN` queries for data extraction and cascaded subqueries for pruning.
4. **Topological Ordering**: Executes **Kahn's Algorithm (Topological Sort)** to produce:
   - **Insert Order**: Root/parent tables first, followed by child tables down to leaves.
   - **Delete Order**: The exact reverse of the insert order (leaf child tables first, root tables last).

### Rebalance Execution Lifecycle & Idempotent Crash Recovery

`RebalanceEngine` (`neko.mukynas.fractal.rebalance.RebalanceEngine`) executes migration actions sequentially per entity with two-phase progress tracking and idempotent crash recovery:

#### In-Flight Migration Tracking (`fractal_tenant_migrations`)

To ensure resilience against mid-migration crashes or node shutdowns, `TopologyManager` maintains an ANSI-compliant tracking table on the primary coordinator database:

```sql
CREATE TABLE IF NOT EXISTS fractal_tenant_migrations (
    tenant_id VARCHAR(255) PRIMARY KEY,
    source_shard VARCHAR(255) NOT NULL,
    target_shard VARCHAR(255) NOT NULL,
    phase VARCHAR(50) NOT NULL,
    started_at TIMESTAMP NOT NULL
);
```

The migration progresses through two distinct phases:
- `COPYING`: Rows are streamed and replicated from the source shard to the target shard in topological insert order.
- `PRUNING`: All rows have been verified on the target shard; residual rows on the source shard are purged in reverse topological order.

#### Migration State Machine

For each tenant identified in the delta plan:

1. **Lock Entity**: Sets the tenant status to `MIGRATING` in memory and DB (via `status-column`). Intercepted requests throw a retryable `TenantMigratingException` to prevent dirty writes and split-brain states.
2. **Record Copy Phase**: Registers the tenant in `fractal_tenant_migrations` with `phase = 'COPYING'`.
3. **Idempotent Target Sanitization**: Before streaming rows, the engine checks whether a prior interrupted attempt left partial rows on the target shard. If the source shard still holds the canonical data, any partial target rows are safely purged in reverse topological order to guarantee clean, idempotent batch inserts.
4. **Data Replication**: Selects all matching rows using the pre-computed migration plans (with foreign key hopping) from the source shard and streams batch inserts into the target shard in chunks of 500 rows.
5. **Record Pruning Phase**: Updates `fractal_tenant_migrations` to `phase = 'PRUNING'`.
6. **Data Eviction**: Removes migrated rows from the source shard in topological delete order (child tables first, root table last).
7. **Clear Migration State**: Deletes the tenant record from `fractal_tenant_migrations`.
8. **Unlock Entity**: Restores the tenant status to `ACTIVE`.

#### Idempotent Restart and Crash Recovery

If the application is stopped, terminated by container orchestration, or crashes due to power outage during migration:

- **Aborted During `COPYING`**: On restart, the source shard remains the intact source of truth. The engine purges any partially inserted records on the target shard and restarts copying from the source. If the copy had completed before the crash, it advances to pruning cleanly.
- **Aborted During `PRUNING`**: On restart, the engine recognizes that the target shard already has all committed tenant data. It avoids duplicate inserts (which would cause unique constraint violations) and directly completes the remaining pruning on the source shard.
- **Already Completed**: If the tenant data exists exclusively on the target shard (`!sourceHasData && targetHasData`), the engine marks the tenant active and cleans up stale records immediately.
- **Repetitive Execution**: Re-running the migration engine multiple times produces identical, zero-side-effect results.

A dedicated single-threaded task executor (`fractalRebalanceExecutor`) configured with `Thread.MIN_PRIORITY` is utilized to avoid saturating request-handling thread pools.

---

## Configuration Reference

### Property Specifications

Configuration keys are grouped under the `fractal.sharding` prefix.

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `fractal.sharding.enabled` | `boolean` | `true` | Enables or disables Fractal auto-configuration. |
| `fractal.sharding.virtual-nodes` | `int` | `150` | Number of virtual points per physical shard on the hash ring. |
| `fractal.sharding.jwt.claim-name` | `String` | `sub` | JWT claim name to extract as sharding key (e.g. `sub`, `tenant_id`, `org_id`). |
| `fractal.sharding.primary.jdbc-url` | `String` | - | JDBC URL for the primary coordination database. |
| `fractal.sharding.primary.username` | `String` | - | Database username for the primary datasource. |
| `fractal.sharding.primary.password` | `String` | - | Database password for the primary datasource. |
| `fractal.sharding.primary.initialize-schema` | `boolean` | `true` | Automatically creates internal coordination tables (`fractal_shard_topology`, `fractal_locks`, `fractal_tenant_migrations`) on primary DB at startup. |
| `fractal.sharding.shards.<name>.jdbc-url` | `String` | - | JDBC URL for physical shard `<name>`. |
| `fractal.sharding.shards.<name>.username` | `String` | - | Database username for physical shard `<name>`. |
| `fractal.sharding.shards.<name>.password` | `String` | - | Database password for physical shard `<name>`. |
| `fractal.sharding.rebalancer.enabled` | `boolean` | `false` | Enables the automatic migration listener on startup. |
| `fractal.sharding.rebalancer.lock-timeout` | `Duration` | `15m` | Maximum lock expiration duration before an unreleased lock is considered dead and eligible for atomic takeover. |
| `fractal.sharding.rebalancer.lock-refresh-interval` | `Duration` | `1m` | Periodic heartbeat interval for renewing `locked_at` during an active rebalance migration. |
| `fractal.sharding.rebalancer.root-table` | `String` | - | Master table holding tenant/entity records (e.g., `organizations`). Inferred from `@ShardedEntity(root = true)` if omitted. |
| `fractal.sharding.rebalancer.root-id-column` | `String` | - | Partition column name (e.g., `org_id`). Inferred from root `@ShardedKey` or `@Id` if omitted. |
| `fractal.sharding.rebalancer.status-column` | `String` | - | Column on `rootTable` indicating migration state. |
| `fractal.sharding.rebalancer.migrating-value` | `String` | `MIGRATING` | State string set during an in-flight migration. |
| `fractal.sharding.rebalancer.active-value` | `String` | `ACTIVE` | State string when tenant is available. |
| `fractal.sharding.rebalancer.sharded-tables` | `List<String>` | `null` | Optional explicit list of sharded tables. Discovered automatically from `@ShardedEntity` domain models or foreign key graph. |
| `fractal.sharding.rebalancer.exclude-tables` | `List<String>` | `null` | Optional list of tables to exclude from auto-discovery. |

### Configuration Example

```yaml
fractal:
  sharding:
    enabled: true
    virtual-nodes: 150
    jwt:
      claim-name: tenant_id       # optional: claim to extract (default: 'sub')
    primary:
      jdbc-url: jdbc:postgresql://localhost:5432/primary_db
      username: postgres
      password: secretpassword
      initialize-schema: true     # auto-creates metadata tables on primary (default: true)
    shards:
      shard-eu-1:
        jdbc-url: jdbc:postgresql://localhost:5433/shard_eu_1
        username: postgres
        password: secretpassword
      shard-eu-2:
        jdbc-url: jdbc:postgresql://localhost:5434/shard_eu_2
        username: postgres
        password: secretpassword
      shard-us-1:
        jdbc-url: jdbc:postgresql://localhost:5435/shard_us_1
        username: postgres
        password: secretpassword
    rebalancer:
      enabled: false
      lock-timeout: 15m           # lock expiration TTL for crash takeover (default: 15m)
      lock-refresh-interval: 1m   # periodic heartbeat to renew lock (default: 1m)
      root-table: organizations
      root-id-column: org_id
      status-column: sync_status
      migrating-value: MIGRATING
      active-value: ACTIVE
      # Optional: if sharded-tables is omitted, descendant tables are auto-discovered via foreign keys
      # sharded-tables:
      #   - organizations
      #   - projects
      #   - audit_logs
      # Optional: exclude specific tables from auto-discovery
      exclude-tables:
        - flyway_schema_history
```

---

## Usage Guide

### Maven Dependency

Add the starter dependency to your project's `pom.xml`:

```xml
<dependency>
    <groupId>neko.mukynas</groupId>
    <artifactId>fractal-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

Ensure compiler parameter retention is enabled so SpEL can resolve argument names:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <parameters>true</parameters>
    </configuration>
</plugin>
```

### Service-Level Annotation with SpEL

Annotate service methods or classes with `@Sharded` and supply a SpEL expression to target method arguments:

```java
package com.example.service;

import neko.mukynas.fractal.annotation.Sharded;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    @Sharded(key = "#tenantId")
    @Transactional
    public Order createOrder(String tenantId, OrderRequest request) {
        // Database queries execute against the shard assigned to tenantId
        return orderRepository.save(new Order(tenantId, request));
    }

    @Sharded(key = "#request.companyId")
    @Transactional(readOnly = true)
    public List<Order> getOrders(OrderQueryRequest request) {
        return orderRepository.findAllByCompany(request.getCompanyId());
    }
}
```

### Transparent Routing via Spring Security JWT

When `spring-boot-starter-oauth2-resource-server` is present on the classpath, `JwtSecurityKeyExtractor` is automatically activated.

By default, the router extracts the JWT `sub` (Subject) claim from the authenticated `JwtAuthenticationToken`. You can configure a custom claim (such as `tenant_id`, `org_id`, or `account_id`) via application properties:

```yaml
fractal:
  sharding:
    jwt:
      claim-name: tenant_id   # extracts 'tenant_id' claim instead of default 'sub'
```

String, numeric, and UUID claim representations are automatically coerced into the routing key string. Service methods require no SpEL annotations:

```java
@Service
public class UserProfileService {

    // Automatically extracts the configured JWT claim from SecurityContextHolder
    @Sharded
    @Transactional(readOnly = true)
    public UserProfile getCurrentUserProfile() {
        return profileRepository.findCurrent();
    }
}
```

### Handling Rebalance Migration Lock (TenantMigratingException)

When a tenant is actively migrating between shards, `ShardingAspect` intercepts service invocations and throws `TenantMigratingException` (`neko.mukynas.fractal.exception.TenantMigratingException`) to prevent dirty writes or split-brain updates while data rows are being moved.

Applications can catch this exception via a Spring `@RestControllerAdvice` and return an HTTP `503 Service Unavailable` or `423 Locked` response with a `Retry-After` header:

```java
package com.example.web;

import neko.mukynas.fractal.exception.TenantMigratingException;
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
                .body("Tenant " + ex.getTenantId() + " is currently migrating between database shards. Please retry shortly.");
    }
}
```

### Implementing a Custom ShardingKeyExtractor

Custom extraction strategies (such as resolving keys from HTTP headers, gRPC metadata, or thread contexts) can be provided by implementing `ShardingKeyExtractor`:

```java
package com.example.config;

import jakarta.servlet.http.HttpServletRequest;
import neko.mukynas.fractal.core.ShardingKeyExtractor;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Component
public class HeaderShardingKeyExtractor implements ShardingKeyExtractor {

    private static final String TENANT_HEADER = "X-Tenant-ID";

    @Override
    public String extractKey() {
        var attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes != null) {
            HttpServletRequest request = attributes.getRequest();
            return request.getHeader(TENANT_HEADER);
        }
        return null;
    }
}
```

---

## Technical Considerations and Architecture Patterns

### Schema Management Best Practices

#### 1. Starter Coordination Tables (Primary Database)
- When `fractal.sharding.primary.initialize-schema` is set to `true` (default), Fractal automatically creates its coordination tables (`fractal_shard_topology`, `fractal_locks`, `fractal_tenant_migrations`) on the primary database at application startup via portable ANSI SQL `CREATE TABLE IF NOT EXISTS`.
- In strict enterprise environments where applications lack DDL privileges at runtime, set `fractal.sharding.primary.initialize-schema: false` and execute the provided DDL script (`src/main/resources/schema-primary.sql`) through your CI/CD database migration pipeline (e.g., Flyway or Liquibase).

#### 2. Business Domain Tables (Physical Shards)
- Fractal is a database routing and rebalancing starter; by design and industry best practices, it does **not** create or alter business domain tables on physical shards.
- Business tables (such as `orders`, `projects`, `tasks`, and `organizations`) must be provisioned identically across all physical shard databases. In Spring Boot applications, this is typically handled by:
  - **Database Migration Tools (Flyway / Liquibase)**: Configured to execute versioned migrations across all shard datasources.
  - **JPA / Hibernate Schema Generation**: Configured with `spring.jpa.hibernate.ddl-auto=update` or `create` during development.
  - **Infrastructure as Code**: Terraform, Ansible, or pre-provisioned container init scripts.

---

### Joining Sharded and Non-Sharded Data

Because sharded data and non-sharded data reside on separate physical database instances and connection pools, a single SQL `JOIN` query cannot be executed across them at the JDBC layer. Three standard architectural patterns address this:

#### Pattern 1: Application-Level Join via Orchestrator Facade (Recommended)

Structure your application using two distinct services coordinated by an orchestrator facade:

1. **Non-Sharded Service**: Omits `@Sharded` and queries global reference data from the `primary` datasource.
2. **Sharded Service**: Uses `@Sharded(key = "#tenantId")` and executes against the tenant's assigned shard database.
3. **Orchestrator / Facade**: Coordinates calls to both services and joins the results in memory using Java streams or DTO mapping.

```java
// 1. Sharded Service: executes queries on tenant's assigned physical shard
@Service
public class OrderService {
    @Autowired private OrderRepository orderRepository;

    @Sharded(key = "#tenantId")
    @Transactional(readOnly = true)
    public List<Order> getOrdersByTenant(String tenantId) {
        return orderRepository.findByTenantId(tenantId);
    }
}

// 2. Non-Sharded Service: executes queries on primary coordination database
@Service
public class CurrencyService {
    @Autowired private CurrencyRateRepository currencyRepository;

    // No @Sharded annotation -> routes to primary datasource
    @Transactional(readOnly = true)
    public Map<String, BigDecimal> getExchangeRates() {
        return currencyRepository.findAllAsRateMap();
    }
}

// 3. Orchestrator Facade: performs the in-memory join in Java
@Service
public class OrderReportingFacade {
    @Autowired private OrderService orderService;
    @Autowired private CurrencyService currencyService;

    // IMPORTANT: Do NOT annotate this orchestrator method with @Transactional.
    // An overarching transaction binds the primary connection before @Sharded evaluates.
    public List<OrderReportDto> getEnrichedOrders(String tenantId) {
        List<Order> orders = orderService.getOrdersByTenant(tenantId);
        Map<String, BigDecimal> rates = currencyService.getExchangeRates();

        return orders.stream()
                .map(order -> new OrderReportDto(order, rates.get(order.getCurrency())))
                .toList();
    }
}
```

> [!IMPORTANT]
> **Transaction Management with Routing DataSources**:
> If the orchestrator facade method itself is marked `@Transactional`, Spring's transaction manager will acquire a connection from the default target datasource (`primary`) *before* `@Sharded` can set the context on the thread. The sharded service will then reuse the already-bound primary connection (`Propagation.REQUIRED`), resulting in missing table errors.
> **Rule**: Place `@Transactional` exclusively on leaf service methods and omit `@Transactional` on the orchestrator facade. For a comprehensive analysis of multi-datasource transaction pitfalls, see [The Transaction Aggregation Problem and Propagation.REQUIRES_NEW Caveats](#the-transaction-aggregation-problem-and-propagationrequires_new-caveats) below.

#### Pattern 2: Broadcast / Replicated Tables (For High-Frequency Lookups)

If the non-sharded data consists of read-mostly reference data (such as currency codes, country lists, categories, or tax rate tables):
- Replicate the reference tables to **every physical shard** using database migration scripts (Flyway) or asynchronous change data capture.
- Sharded service methods can then execute native, high-performance SQL `JOIN` queries directly inside the local shard database.

#### Pattern 3: In-Memory / Distributed Caching

Cache global reference data in Redis or local memory using Spring Cache (`@Cacheable`). The sharded service queries only the shard database and enriches DTOs using cached lookup values without querying the primary database.

---

### The Transaction Aggregation Problem and Propagation.REQUIRES_NEW Caveats

When coordinating or aggregating data across multiple datasources (e.g., between the primary coordination DB and one or more physical shards, or across multiple shards), application developers face fundamental transaction management challenges inherent to Spring's architecture.

#### The Transaction Aggregation Problem (Routing Lock-In)

1. **Single Connection Affinity per Transaction**:
   Spring's transaction infrastructure (`DataSourceTransactionManager` and `JpaTransactionManager`) binds exactly one physical JDBC `Connection` to the current thread for the duration of a `@Transactional` boundary via `TransactionSynchronizationManager`.
2. **Early Connection Acquisition and Routing Lock-In**:
   When an orchestrator facade or parent service method opens a `@Transactional` block, Spring eagerly acquires a connection from `ShardingRoutingDataSource` *before* child `@Sharded` annotations are evaluated. Because no sharding key has been established yet, `determineCurrentLookupKey()` returns `null`, locking the entire transaction onto the default `primary` datasource.
3. **Silent Target Misdirection**:
   When the orchestrator subsequently calls a child `@Sharded` method, the `ShardingAspect` intercepts the call and updates `ShardContextHolder`. However, Spring's transaction manager **never calls `determineCurrentLookupKey()` again**, because a connection is already bound to the active transaction (`Propagation.REQUIRED`). Consequently, queries intended for a physical shard are silently routed to the `primary` database, producing `Table not found` errors or data corruption.

#### The Pitfalls and Dangers of `Propagation.REQUIRES_NEW`

Developers often attempt to bypass this routing lock-in by annotating child service methods with `@Transactional(propagation = Propagation.REQUIRES_NEW)`. While this forces Spring to suspend the outer transaction and obtain a new connection from `ShardingRoutingDataSource` with the shard key active, it introduces severe architectural and operational hazards:

1. **Atomicity Breakdown & Lack of Distributed Rollback (Partial Failures)**:
   - `Propagation.REQUIRES_NEW` creates completely independent, autonomous physical database transactions.
   - If the inner transaction on the shard commits successfully, but the outer transaction (or a subsequent call to primary or another shard) subsequently throws a `RuntimeException`, **the committed inner transaction CANNOT be rolled back**.
   - Without an XA / Two-Phase Commit (2PC) coordinator, this breaks ACID atomicity and leaves the system in an inconsistent, split-brain state (for example: an order record is committed on the shard, but user balance deduction on primary rolls back).
2. **Connection Pool Starvation and Application Deadlock**:
   - When an outer transaction holds a connection from the primary pool and calls a method with `REQUIRES_NEW` that requests another connection from the same pool or a shard pool, a single worker thread holds **multiple physical database connections concurrently**.
   - Under moderate to high concurrent load, if all worker threads in the web container acquire outer connections, none of them can obtain their inner `REQUIRES_NEW` connection because pool capacity is exhausted. This produces a **classic connection pool deadlock**, freezing the entire application until connection acquisition timeouts fail every request.
3. **Isolation Anomalies and Dirty Reads**:
   - The inner transaction operates on an independent physical connection and cannot observe uncommitted writes made by the outer transaction.
   - Conversely, once the inner transaction commits, its modifications become immediately visible to concurrent requests across the system, even if the outer business workflow subsequently aborts.

#### Recommended Architectural Best Practices

- **For Read Operations (In-Memory Aggregations & Joins)**:
  - **Omit `@Transactional` on the Orchestrator/Facade**: The orchestrator should never declare a transaction.
  - **Localize Transactions to Leaf Services**: Keep `@Transactional(readOnly = true)` exclusively on individual leaf service methods (`OrderService`, `CurrencyService`). Each service acquires and immediately releases its connection, eliminating connection hoarding and pool exhaustion.
- **For Write Operations (Multi-Shard / Cross-Database Mutations)**:
  - **Never rely on nested `REQUIRES_NEW` for cross-database atomicity**.
  - **Implement the Saga Pattern**: Structure cross-database workflows as sequential local transactions, accompanied by explicit compensating transactions to undo earlier steps if a downstream failure occurs.
  - **Use the Transactional Outbox Pattern**: Commit business mutations and an outbox event in a single local transaction on the shard, and propagate changes to the primary DB or other shards asynchronously via an event broker (e.g., Apache Kafka or RabbitMQ) with guaranteed at-least-once delivery.

---

## Technical Considerations and Dialect Constraints

- **ANSI SQL Schema Catalog in Rebalancer**: `TableDependencyResolver` inspects standard ANSI `information_schema.referential_constraints` and `information_schema.key_column_usage` views. This conforms to ANSI SQL standards and is fully supported on PostgreSQL, H2, and modern SQL engines.
- **Cross-Shard Queries**: Fractal is an application-level routing starter. Joins across different physical shards are not supported at the JDBC layer and must be aggregated at the application layer as described above.
- **Global / Unpartitioned Entities**: Entities not mapped to a tenant or partition key reside on the `primary` datasource by default when accessed outside `@Sharded` methods.

---

## Building and Testing

### Prerequisites

- Java Development Kit (JDK) 17 or higher
- Apache Maven 3.8+

### Execution

Execute the unit and integration test suite:

```bash
mvn clean test
```

The test suite covers 38 automated tests across 12 test suites:
- `ConsistentHashRouterTest`: Validates deterministic routing and uniform key distribution across virtual nodes on the 64-bit ring.
- `RoutingIntegrationTest`: Verifies dynamic shard selection, primary fallback, and SpEL resolution using in-memory H2 databases.
- `SecurityRoutingIntegrationTest`: Confirms end-to-end routing using synthetic JWT security tokens (`sub` claim) in `SecurityContextHolder`.
- `CustomClaimSecurityRoutingIntegrationTest`: Tests end-to-end shard routing using custom JWT claims (e.g. `tenant_id`).
- `JwtSecurityKeyExtractorTest`: Validates custom claim extraction, fallback logic, string/numeric/UUID claim conversions, and unauthenticated state handling.
- `ShardingAspectMigrationLockTest`: Asserts that `TenantMigratingException` is thrown when accessing a tenant currently flagged as migrating.
- `TableDependencyResolverTest`: Verifies ANSI `information_schema` foreign key discovery, multi-hop BFS dependency resolution (`users` -> `projects` -> `tasks`), Kahn's topological sort for insert/delete ordering, join query synthesis, and table exclusion.
- `EntityTableMetadataResolverTest`: Validates domain entity auto-discovery via `@ShardedEntity` and `@ShardedKey`, multi-tier hierarchy resolution, JPA `@Table`/`@JoinColumn`/`@Column`/`@Id` metadata extraction, cycle detection, reachability checks, and single-root enforcement.
- `EntityRebalanceIntegrationTest`: Confirms end-to-end multi-hop tenant migration on physical databases without database foreign key constraints using entity-discovered plans.
- `RebalanceEngineTest`: Validates end-to-end tenant migration, row copying across shards using multi-hop plans, and reverse-order row pruning.
- `TopologyManagerLockTest`: Verifies distributed lock acquisition, mutual exclusion, expired lock takeover via configurable TTL, periodic heartbeat renewal, graceful shutdown lock release, automatic schema initialization via `InitializingBean`, and idempotent ANSI shard registration.
- `RebalanceEngineIdempotencyTest`: Asserts idempotent crash recovery across migration phases, resumption from aborted `COPYING` state (purging partial target data and recopying), resumption from aborted `PRUNING` state (safe source pruning without duplicate inserts), and safe repeated execution without side effects.




