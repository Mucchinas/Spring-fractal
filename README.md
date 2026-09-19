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
  - [Topology Management and Distributed Locking](#topology-management-and-distributed-locking)
  - [Delta Calculation](#delta-calculation)
  - [Domain Entity Auto-Discovery (@ShardedEntity & @ShardedKey)](#domain-entity-auto-discovery-shardedentity--shardedkey)
  - [Database Catalog Dependency Resolution](#database-catalog-dependency-resolution)
  - [Rebalance Execution Lifecycle](#rebalance-execution-lifecycle)
- [Configuration Reference](#configuration-reference)
  - [Property Specifications](#property-specifications)
  - [Configuration Example](#configuration-example)
- [Usage Guide](#usage-guide)
  - [Maven Dependency](#maven-dependency)
  - [Service-Level Annotation with SpEL](#service-level-annotation-with-spel)
  - [Transparent Routing via Spring Security JWT](#transparent-routing-via-spring-security-jwt)
  - [Handling Rebalance Migration Lock (TenantMigratingException)](#handling-rebalance-migration-lock-tenantmigratingexception)
  - [Implementing a Custom ShardingKeyExtractor](#implementing-a-custom-shardingkeyextractor)
- [Technical Considerations and Dialect Constraints](#technical-considerations-and-dialect-constraints)
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
public class ShardingAspect { ... }
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

### Topology Management and Distributed Locking

`TopologyManager` (`neko.mukynas.fractal.rebalance.TopologyManager`) coordinates rebalancing across clustered application nodes:

- **`fractal_shard_topology`**: Persists the known active shards and their statuses.
- **`fractal_locks`**: Implements mutual exclusion across multiple application instances using primary key constraint semantics:
  ```sql
  INSERT INTO fractal_locks (lock_name, locked_by) VALUES ('REBALANCE_LOCK', ?)
  ```
  If another pod holds the lock, the insert fails and execution yields.

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

### Rebalance Execution Lifecycle

`RebalanceEngine` (`neko.mukynas.fractal.rebalance.RebalanceEngine`) executes migration actions sequentially per entity:

1. **Lock Entity**: Sets the tenant status to `MIGRATING` in `TopologyManager` and the primary database. Any requests intercepted by `ShardingAspect` for this tenant throw a retryable `TenantMigratingException` to prevent dirty writes and split-brain states.
2. **Data Replication**: Selects all matching rows using the pre-computed migration plans (with foreign key hopping) from the source shard and streams batch inserts into the target shard in chunks of 500 rows.
3. **Data Eviction**: Removes the migrated rows from the source shard in topological delete order.
4. **Unlock Entity**: Restores the tenant status to `ACTIVE`.

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
| `fractal.sharding.shards.<name>.jdbc-url` | `String` | - | JDBC URL for physical shard `<name>`. |
| `fractal.sharding.shards.<name>.username` | `String` | - | Database username for physical shard `<name>`. |
| `fractal.sharding.shards.<name>.password` | `String` | - | Database password for physical shard `<name>`. |
| `fractal.sharding.rebalancer.enabled` | `boolean` | `false` | Enables the automatic migration listener on startup. |
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

## Technical Considerations and Dialect Constraints

- **ANSI SQL Schema Catalog in Rebalancer**: `TableDependencyResolver` inspects standard ANSI `information_schema.referential_constraints` and `information_schema.key_column_usage` views. This conforms to ANSI SQL standards and is fully supported on PostgreSQL, H2, and modern SQL engines. Dialects that deviate from ANSI standard information schema definitions can either configure `sharded-tables` explicitly or supply custom metadata extraction.
- **Cross-Shard Queries**: Fractal is an application-level routing mechanism. Joins or cross-table queries across different physical shards are not supported at the JDBC layer and must be aggregated at the application layer.
- **Global / Unpartitioned Entities**: Entities not mapped to a tenant or partition key must either reside on the `primary` datasource or use a dedicated routing aspect.
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

The test suite covers 28 automated tests across 10 test suites:
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


