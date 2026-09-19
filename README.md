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
  - [Foreign Key Dependency Resolution](#foreign-key-dependency-resolution)
  - [Rebalance Execution Lifecycle](#rebalance-execution-lifecycle)
- [Configuration Reference](#configuration-reference)
  - [Property Specifications](#property-specifications)
  - [Configuration Example](#configuration-example)
- [Usage Guide](#usage-guide)
  - [Maven Dependency](#maven-dependency)
  - [Service-Level Annotation with SpEL](#service-level-annotation-with-spel)
  - [Transparent Routing via Spring Security JWT](#transparent-routing-via-spring-security-jwt)
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
|     - Priority A: ShardingKeyExtractor chain (e.g. JWT 'sub' claim)   |
|     - Priority B: SpEL expression evaluation on method arguments       |
|                                                                       |
|  2. ConsistentHashRouter:                                             |
|     - Hash key with MD5 onto 64-bit virtual node ring                 |
|     - Resolve target shard name (e.g., "shard-1")                     |
|                                                                       |
|  3. ShardContextHolder:                                               |
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

### Foreign Key Dependency Resolution & Auto-Discovery

`TableDependencyResolver` (`neko.mukynas.fractal.rebalance.TableDependencyResolver`) introspects database foreign key constraints using ANSI standard `information_schema` tables:

1. **Zero-Config Auto-Discovery**: If `sharded-tables` is omitted from configuration, Fractal recursively traces foreign key relationships starting from `rootTable`, automatically identifying all child, grandchild, and descendant tables.
2. **Exclusion Filtering**: Tables can be excluded from auto-discovery via `exclude-tables`.
3. **Foreign Key Hopping**: For tables without a direct root foreign key (e.g. `users` -> `projects` -> `tasks`), the resolver generates relational `JOIN` queries for data extraction and cascaded subqueries for pruning.
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
| `fractal.sharding.rebalancer.root-table` | `String` | - | Master table holding tenant/entity records (e.g., `tenants`). |
| `fractal.sharding.rebalancer.root-id-column` | `String` | - | Partition column name present across tables (e.g., `tenant_id`). |
| `fractal.sharding.rebalancer.status-column` | `String` | - | Column on `rootTable` indicating migration state. |
| `fractal.sharding.rebalancer.migrating-value` | `String` | `MIGRATING` | State string set during an in-flight migration. |
| `fractal.sharding.rebalancer.active-value` | `String` | `ACTIVE` | State string when tenant is available. |
| `fractal.sharding.rebalancer.sharded-tables` | `List<String>` | `null` | Optional explicit list of sharded tables. If omitted, discovered automatically from `rootTable`. |
| `fractal.sharding.rebalancer.exclude-tables` | `List<String>` | `null` | Optional list of tables to exclude from auto-discovery. |

### Configuration Example

```yaml
fractal:
  sharding:
    enabled: true
    virtual-nodes: 150
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
      sharded-tables:
        - organizations
        - projects
        - audit_logs
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

When `spring-boot-starter-oauth2-resource-server` is present on the classpath, `JwtSecurityKeyExtractor` is automatically activated. The router will extract the JWT `sub` (Subject) claim without requiring SpEL expressions on the method:

```java
@Service
public class UserProfileService {

    // Automatically extracts the JWT 'sub' claim from SecurityContextHolder
    @Sharded
    @Transactional(readOnly = true)
    public UserProfile getCurrentUserProfile() {
        return profileRepository.findCurrent();
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

- **PostgreSQL Dependency in Rebalancer**: `TableDependencyResolver` queries PostgreSQL-specific constraint tables (`information_schema.table_constraints`, `key_column_usage`, `constraint_column_usage`). If rebalancing is used with MySQL, MariaDB, or Oracle, alternative metadata extraction queries or manual dependency ordering must be provided.
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

The test suite includes:
- `ConsistentHashRouterTest`: Validates deterministic routing and uniform key distribution across virtual nodes.
- `RoutingIntegrationTest`: Verifies dynamic shard selection and SpEL resolution using in-memory H2 databases.
- `SecurityRoutingIntegrationTest`: Confirms end-to-end routing using synthetic JWT security tokens in `SecurityContextHolder`.
