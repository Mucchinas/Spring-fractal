# Fractal Deep Dive & Architecture Reference

This document provides an exhaustive, in-depth technical analysis of the internal mechanics, distributed coordination protocols, migration state machines, and advanced architectural patterns implemented by the Fractal Spring Boot Starter.

For a quick setup and introductory guide, refer to the [README.md](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/README.md).

---

## Table of Contents

1. [Architectural Overview & Request Interception Mechanics](#1-architectural-overview--request-interception-mechanics)
   - [Request Routing Lifecycle](#request-routing-lifecycle)
   - [Spring AOP Interception Precedence](#spring-aop-interception-precedence)
   - [ThreadLocal Lifecycle & Connection Leasing](#threadlocal-lifecycle--connection-leasing)
2. [Core Components Deep Dive](#2-core-components-deep-dive)
   - [Consistent Hash Router & Ring Distribution](#consistent-hash-router--ring-distribution)
   - [Sharding Routing DataSource & HikariCP Pools](#sharding-routing-datasource--hikaricp-pools)
   - [Sharding Key Extraction Pipeline](#sharding-key-extraction-pipeline)
3. [Automated Rebalancer & Migration Subsystem](#3-automated-rebalancer--migration-subsystem)
   - [Distributed Coordination, Locking, & Heartbeat](#distributed-coordination-locking--heartbeat)
   - [High-Performance In-Memory Migration Cache (Caffeine) & Root Catalog Architecture](#high-performance-in-memory-migration-cache-caffeine--root-catalog-architecture)
   - [Topology Delta Calculation Engine](#topology-delta-calculation-engine)
   - [Domain Entity Auto-Discovery (@ShardedEntity, @ShardedKey, @ShardedStatus)](#domain-entity-auto-discovery-shardedentity-shardedkey-shardedstatus)
   - [Database Catalog Dependency Resolution (ANSI Information Schema)](#database-catalog-dependency-resolution-ansi-information-schema)
   - [Rebalance Execution Lifecycle & Two-Phase State Machine](#rebalance-execution-lifecycle--two-phase-state-machine)
   - [Idempotent Crash Recovery & Resumption Mechanics](#idempotent-crash-recovery--resumption-mechanics)
4. [Use Cases & Reference Configurations](#4-use-cases--reference-configurations)
   - [Architecture Profile Matrix](#architecture-profile-matrix)
   - [Profile 1: B2B Multi-Tenant SaaS (Zero-Config)](#profile-1-b2b-multi-tenant-saas-zero-config)
   - [Profile 2: High-Throughput User / Account Partitioning (SpEL)](#profile-2-high-throughput-user--account-partitioning-spel)
   - [Profile 3: Turnkey Legacy Catalog Sharding (shard-all)](#profile-3-turnkey-legacy-catalog-sharding-shard-all)
   - [Profile 4: Read-Mostly Reference Replication & Broadcasting](#profile-4-read-mostly-reference-replication--broadcasting)
   - [Profile 5: Asynchronous Worker / Event-Consumer Nodes](#profile-5-asynchronous-worker--event-consumer-nodes)
5. [Configuration Property Specifications](#5-configuration-property-specifications)
   - [Comprehensive Property Matrix](#comprehensive-property-matrix)
   - [Exhaustive application.yml Reference](#exhaustive-applicationyml-reference)
6. [Advanced Usage & Integration Patterns](#6-advanced-usage--integration-patterns)
   - [Transparent Routing via Spring Security JWT](#transparent-routing-via-spring-security-jwt)
   - [Handling Rebalance Migration Locks (TenantMigratingException)](#handling-rebalance-migration-locks-tenantmigratingexception)
   - [Implementing a Custom ShardingKeyExtractor](#implementing-a-custom-shardingkeyextractor)
7. [Technical Considerations, Pitfalls & Solutions](#7-technical-considerations-pitfalls--solutions)
   - [Dual-Presence Schema Management & Flyway/Liquibase Best Practices](#dual-presence-schema-management--flywayliquibase-best-practices)
   - [Joining Sharded and Non-Sharded Data](#joining-sharded-and-non-sharded-data)
   - [The Transaction Aggregation Problem and Propagation.REQUIRES_NEW Caveats](#the-transaction-aggregation-problem-and-propagationrequires_new-caveats)
   - [Database Dialect Constraints & ANSI SQL Compatibility](#database-dialect-constraints--ansi-sql-compatibility)
8. [Verification, Testing & Test Suite Reference](#8-verification-testing--test-suite-reference)

---

## 1. Architectural Overview & Request Interception Mechanics

Fractal transparently partitions data across heterogeneous physical database clusters without requiring modifications to standard Spring Data JPA repositories or plain JDBC templates.

### Request Routing Lifecycle

Dynamic shard selection operates at the boundary between Spring's AOP proxy infrastructure and the JDBC driver manager:

```
+-----------------------------------------------------------------------+
|                            Client Request                             |
|          (HTTP REST Request / Scheduled Job / Event Consumer)         |
+-----------------------------------+-+---------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                 Spring AOP Proxy (@Sharded Interceptor)                |
|               [Order(1)] - Before Transactional Manager               |
|                                                                       |
|  1. Extract Sharding Key:                                             |
|     - Priority A: ShardingKeyExtractor chain (e.g. JWT claim)          |
|     - Priority B: SpEL expression evaluation on method arguments       |
|                                                                       |
|  2. Tenant Migration Guard (Zero-Overhead Caffeine In-Memory Cache):   |
|     - TopologyManager.isTenantMigrating(key)                          |
|     - Fast path: Caffeine in-memory cache lookup (~15 nanoseconds)    |
|     - Slow path: Authoritative query to Primary DB on cache miss/TTL   |
|     - Throws TenantMigratingException if migration is in flight       |
|                                                                       |
|  3. ConsistentHashRouter:                                             |
|     - Hash key with MD5 onto 64-bit virtual node ring                 |
|     - Resolve target shard name (e.g., "shard-1")                     |
|                                                                       |
|  4. ShardContextHolder:                                               |
|     - Bind shard name to ThreadLocal context                          |
+-----------------------------------+-+---------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                 Spring @Transactional Boundary                        |
|               [Ordered.LOWEST_PRECEDENCE]                             |
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

### Spring AOP Interception Precedence

The order of interception between [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) and Spring's `TransactionInterceptor` is critical:

```java
@Aspect
@Order(1)
public class ShardingAspect {
    // Interception logic
}
```

In standard Spring configurations, `@Transactional` aspects execute with default lowest precedence (`Ordered.LOWEST_PRECEDENCE`). By annotating [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) with `@Order(1)`, Fractal guarantees:
1. The sharding key is parsed and routed, and the resolved shard identifier is committed to [`ShardContextHolder`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/core/ShardContextHolder.java) *prior* to `DataSourceTransactionManager` or `JpaTransactionManager` invoking `getConnection()`.
2. If [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) executed with lower precedence than `@Transactional`, Spring's transaction manager would acquire a connection before the sharding key is set. In that erroneous sequence, `determineCurrentLookupKey()` would return `null`, erroneously binding the entire transaction to the default `primary` datasource.

### ThreadLocal Lifecycle & Connection Leasing

[`ShardContextHolder`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/core/ShardContextHolder.java) stores the resolved shard identifier in a thread-local variable:

```java
public final class ShardContextHolder {
    private static final ThreadLocal<String> CURRENT_SHARD = new ThreadLocal<>();

    public static void setShard(String shardName) { CURRENT_SHARD.set(shardName); }
    public static String getShard() { return CURRENT_SHARD.get(); }
    public static void clear() { CURRENT_SHARD.remove(); }
}
```

To eliminate memory leaks and cross-request contamination in pooled thread environments (e.g. Tomcat, Undertow, or virtual threads), [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) wraps method invocations in a strict `try ... finally` block:
- **`try`**: Extracts the key, checks the migration cache, resolves the shard via [`ConsistentHashRouter`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/core/ConsistentHashRouter.java), and calls `ShardContextHolder.setShard(targetShard)`.
- **`finally`**: Unconditionally calls `ShardContextHolder.clear()`. Even if downstream service code or transaction commits throw an unhandled `RuntimeException`, the thread context is completely sanitized before the thread returns to the server pool.

---

## 2. Core Components Deep Dive

### Consistent Hash Router & Ring Distribution

[`ConsistentHashRouter`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/core/ConsistentHashRouter.java) implements a virtual node consistent hashing ring mapped across the signed 64-bit integer space ($[-2^{63}, 2^{63}-1]$).

```
                             Ring Structure (64-bit space)
                                        0
                                  . - ~ ~ ~ - .
                              .                   .
                            .                       .
                          .                           .
                        .    [shard-1-VN-42]            .
                       .                                 .
                      .                                   .
                     .                                     .
        -2^63 =======|                                     |======= 2^63 - 1
                     .     [shard-2-VN-87]                 .
                      .                                   .
                       .            [shard-1-VN-0]       .
                        .                               .
                          .                           .
                            .                       .
                              .                   .
                                  . - ~ ~ ~ - .
```

#### Key Design Characteristics:
1. **Virtual Node Factor (`virtualNodes`, default: `150`)**:
   - Each physical shard registers $V$ virtual positions on the ring with the token key `<shardName>-VN-<i>`.
   - With 150 virtual nodes per shard, the standard deviation of key distribution drops below $5\%$, preventing hot spots and uneven disk utilization across physical hardware.
2. **Ring Storage**:
   - Implemented as an immutable or synchronized `java.util.TreeMap<Long, String>`.
3. **Cryptographic Hashing**:
   - The key string is digested with `MD5`.
   - The first 8 bytes of the digest are packed into a 64-bit `Long` via bitwise operations:
     $$\text{hash} = \sum_{i=0}^{7} (\text{digest}[i] \ \& \ 0\text{xFF}) \ll (8 \times (7 - i))$$
4. **Binary Search Traversal ($O(\log(N \times V))$)**:
   - Shard lookup calls `ring.tailMap(hash)`.
   - If `tailMap.isEmpty()` is true, traversal wraps around clockwise to `ring.firstKey()`.
5. **Minimal Key Relocation**:
   - When a new physical shard is introduced, only $\frac{K}{N+1}$ keys are relocated (where $K$ is the total key count and $N$ is the number of active shards), leaving all other mappings undisturbed.

### Sharding Routing DataSource & HikariCP Pools

[`ShardingRoutingDataSource`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/datasource/ShardingRoutingDataSource.java) subclasses Spring JDBC's `AbstractRoutingDataSource`:

1. **Independent Connection Pools**:
   - For every physical shard declared under `fractal.sharding.shards`, an isolated `HikariDataSource` pool is instantiated and registered in a target data source dictionary (`Map<Object, Object>`).
   - A dedicated `HikariDataSource` pool is created for the `primary` database.
2. **Dynamic Key Resolution**:
   - Overrides `determineCurrentLookupKey()`, which returns `ShardContextHolder.getShard()`.
3. **Fallback Target**:
   - The primary data source is set as `defaultTargetDataSource`. Any un-sharded database interaction or unannotated service invocation automatically defaults to the primary database.

### Sharding Key Extraction Pipeline

[`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) resolves sharding keys via an ordered multi-tier fallback pipeline:

```
Method Invocation
       |
       v
[Tier 1: Strategic Extractors] ---> Returns non-empty key? ---> (YES) ---> Route Key
       | (NO)
       v
[Tier 2: Service Method SpEL]  ---> Expression evaluates?  ---> (YES) ---> Route Key
       | (NO)
       v
[Tier 3: Fail-Fast Guard]      ---> Throw IllegalStateException
```

1. **Tier 1 - Strategic Extractors (`ShardingKeyExtractor`)**:
   - Injects an ordered stream of `ShardingKeyExtractor` beans (`ObjectProvider<ShardingKeyExtractor>`).
   - For instance, [`JwtSecurityKeyExtractor`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/security/JwtSecurityKeyExtractor.java) extracts the designated tenant claim from Spring Security's `SecurityContextHolder`.
   - The first extractor that yields a non-blank string supplies the routing key.
2. **Tier 2 - SpEL Expression Parser**:
   - Evaluates the `@Sharded(key = "...")` expression against method arguments.
   - Leverages `SpelExpressionParser` and `StandardEvaluationContext` backed by `DefaultParameterNameDiscoverer`.
   - Supports parameter identifiers (e.g. `#tenantId`), nested properties (`#request.company.id`), and complex SpEL expressions.
3. **Tier 3 - Fail-Fast Guard**:
   - If neither a strategic extractor nor SpEL evaluation produces a valid key, execution halts immediately with an `IllegalStateException`, aborting the transaction before any database connection is acquired.

---

## 3. Automated Rebalancer & Migration Subsystem

Adding new physical shards requires transferring existing tenants and their relational data graphs without taking down the cluster or corrupting in-flight user operations.

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
                    |  TableDependencyResolver / MetadataResolver|
                    |  Graph Topological Sort (Insert & Delete)  |
                    +---------------------+----------------------+
                                          |
                                          v
                    +--------------------------------------------+
                    |  RebalanceEngine:                          |
                    |  1. UPDATE root SET status = 'MIGRATING'   |
                    |  2. Drain in-flight local transactions     |
                    |  3. Batch copy rows in topological order   |
                    |  4. Batch delete rows in reverse order     |
                    |  5. UPDATE root SET status = 'ACTIVE'      |
                    +---------------------+----------------------+
                                          |
                                          v
                    +--------------------------------------------+
                    |  TopologyManager: releaseRebalanceLock     |
                    |  Record new shards in fractal_shard_topology|
                    +--------------------------------------------+
```

### Distributed Coordination, Locking, & Heartbeat

[`TopologyManager`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TopologyManager.java) orchestrates rebalance operations across distributed application instances using relational coordination tables on the primary database:

#### Coordination Schemas:
- **`fractal_shard_topology`**: Maintains registered physical shard names and their cluster membership state.
- **`fractal_locks`**: Guarantees mutual exclusion across application replicas using primary key semantics:
  ```sql
  CREATE TABLE IF NOT EXISTS fractal_locks (
      lock_name VARCHAR(255) PRIMARY KEY,
      locked_by VARCHAR(255) NOT NULL,
      locked_at TIMESTAMP NOT NULL
  );
  ```

#### 1. Atomic Acquisition:
When a node detects a topology delta, it attempts an atomic `INSERT`:
```sql
INSERT INTO fractal_locks (lock_name, locked_by, locked_at)
VALUES ('REBALANCE_LOCK', ?, ?)
```

#### 2. Lock TTL & Atomic Crash Takeover:
If an application pod dies abruptly (e.g., OOMKilled, SIGKILL, hardware failure) while holding the lock, the lock record remains in the database. To prevent deadlock without DBA intervention:
- If the `INSERT` fails with a primary key collision, [`TopologyManager`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TopologyManager.java) evaluates whether the lock has exceeded `fractal.sharding.rebalancer.lock-timeout` (default: `15m`).
- If expired, the node attempts an atomic conditional `UPDATE`:
  ```sql
  UPDATE fractal_locks
  SET locked_by = ?, locked_at = ?
  WHERE lock_name = 'REBALANCE_LOCK' AND locked_at < ?
  ```
  where the timestamp threshold is `now - lockTimeout`. If rows are affected ($1$), the dead lock is atomically commandeered.

#### 3. Periodic Heartbeat Daemon:
For large datasets where batch migrations exceed standard timeouts, a background heartbeat daemon thread refreshes the lock timestamp at a fixed interval (`fractal.sharding.rebalancer.lock-refresh-interval`, default: `1m`):
```sql
UPDATE fractal_locks
SET locked_at = CURRENT_TIMESTAMP
WHERE lock_name = 'REBALANCE_LOCK' AND locked_by = ?
```

#### 4. Graceful Shutdown Hook:
[`TopologyManager`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TopologyManager.java) implements Spring's `DisposableBean`. Upon receiving `SIGTERM`:
1. The heartbeat daemon scheduler is terminated cleanly.
2. The `REBALANCE_LOCK` row is deleted immediately from `fractal_locks`.
3. The migration thread executor (`fractalRebalanceExecutor`) is configured with `setWaitForTasksToCompleteOnShutdown(true)` and `setAwaitTerminationSeconds(30)`, permitting in-flight batch copies to flush cleanly before termination.

---

### High-Performance In-Memory Migration Cache (Caffeine) & Root Catalog Architecture

Checking whether a tenant is actively migrating on every HTTP or service invocation could bottleneck the primary coordinator database. Fractal solves this using a **Dual-Presence Root Table Architecture** backed by a **Caffeine In-Memory Cache**:

#### Dual-Presence Root Table Architecture

```
                       Primary Database
              +----------------------------------+
              |      Master Tenant Catalog       |
              |       (e.g. organizations)       |
              |   org_id | name | sync_status    |
              |   -------+------+-------------   |
              |    org-1 | Acme | ACTIVE         |
              |    org-2 | Beta | MIGRATING      |
              +-----------------+----------------+
                                |
          +---------------------+---------------------+
          |                                           |
          v                                           v
    Shard 1 (DB)                                Shard 2 (DB)
+------------------------+                  +------------------------+
| Local Root Slice       |                  | Local Root Slice       |
| (org-1 row only)       |                  | (org-2 row only)       |
|                        |                  |                        |
| Child Tables (FK):     |                  | Child Tables (FK):     |
| - projects (FK org-1)  |                  | - projects (FK org-2)  |
| - tasks    (FK proj)   |                  | - tasks    (FK proj)   |
+------------------------+                  +------------------------+
```

1. **Primary Database (Master Root Catalog)**:
   - Holds the master definition of the root entity (e.g. `organizations`).
   - Serves as the authoritative source of truth for tenant existence and global migration status (`MIGRATING` vs. `ACTIVE`).
2. **Physical Shards (Local Shard Slices)**:
   - Each shard maintains the root table schema, populated strictly with the subset of tenants resident on that shard.
   - Enables native relational joins (e.g. `SELECT * FROM projects p JOIN organizations o ON p.org_id = o.id`) and database-level foreign key cascades directly on the shard.
3. During rebalancing, [`RebalanceEngine`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/RebalanceEngine.java) streams the tenant root row along with all descendant rows from the source shard to the target shard and removes io from the source shard, while the master record on the Primary Database remains permanently intact.

#### Sub-Microsecond Cache-Aside Migration Guard

[`TopologyManager`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TopologyManager.java) caches tenant migration statuses using [Caffeine](https://github.com/ben-manes/caffeine):

- **Fast Path (~15 Nanoseconds)**: Service methods check `TopologyManager.isTenantMigrating(tenantId)` directly in JVM heap memory with zero network or JDBC overhead.
- **Slow Path (Authoritative Lookup)**: On a cache miss or TTL expiration (`status-cache-ttl`, default: `2s`), Fractal queries `fractal_tenant_migrations` and the master root table on the Primary DB.
- **Proactive Local Cache Priming**:
  - `markTenantMigrating(id)`: Immediately writes `true` to local cache.
  - `markTenantActive(id)`: Immediately writes `false` to local cache.
  - `clearMigrationRecord(id)`: Explicitly invalidates the cache key.
- **Drain Timeout Safety Invariant**:
  By default, `status-cache-ttl: 2s` and `drain-timeout: 10s` (enforced minimum: `5s`). Because:
  $$\text{drainTimeout} \ge \text{statusCacheTtl} + \text{network RTT}$$
  any stale cached `ACTIVE` entry across clustered nodes is guaranteed to expire and refresh to `MIGRATING` from the Primary DB long before the rebalancer begins deleting data from the source shard.

---

### Topology Delta Calculation Engine

[`MigrationDeltaCalculator`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/MigrationDeltaCalculator.java) constructs two concurrent consistent hashing rings:
1. **Old Topology Ring**: Initialized from active shards stored in `fractal_shard_topology`.
2. **New Topology Ring**: Initialized from all shards configured in `application.yml`.

It iterates through all tenant identifiers in the master root table on the primary database:
- Evaluates $\text{oldShard} = \text{oldRouter.routeNode}(\text{id})$
- Evaluates $\text{newShard} = \text{newRouter.routeNode}(\text{id})$
- When $\text{oldShard} \neq \text{newShard}$, io records a `MigrationAction(id, oldShard, newShard)`.
- Rebalance planning runs in $O(T \log(N \cdot V))$ time (where $T$ is tenant count).

---

### Domain Entity Auto-Discovery (@ShardedEntity, @ShardedKey, @ShardedStatus)

Fractal provides declarative entity auto-discovery via [`EntityTableMetadataResolver`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/EntityTableMetadataResolver.java):

```java
@Entity
@Table(name = "organizations")
@ShardedEntity(root = true)
public class Organization {
    @Id
    @ShardedKey
    private String id;

    private String name;

    @ShardedStatus(migratingValue = "MIGRATING", activeValue = "ACTIVE")
    @Column(name = "sync_status")
    private String syncStatus;
}

@Entity
@Table(name = "projects")
@ShardedEntity
public class Project {
    @Id
    private UUID id;

    // Direct object reference: target entity inferred as Organization
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

    // Scalar foreign key: points explicitly to Project
    @ShardedKey(targetEntity = Project.class, column = "project_id")
    private UUID projectId;
}
```

#### Discovery Mechanics:
1. **Root Partition Anchor**: Exactly one entity must have `@ShardedEntity(root = true)`. Its `@ShardedKey` field defines `rootTable` and `rootIdColumn`.
2. **Foreign Key Hopping**: Descendant entities specify `@ShardedKey` on entity references (`@ManyToOne`) or scalar fields (`targetEntity = ...`).
3. **Status Column Discovery**: `@ShardedStatus` designates the tenant status column on the root entity. Column names and values (`migratingValue`, `activeValue`) are inferred from JPA `@Column` or annotations.
4. **Physical FK Independence**: The entity dependency graph is constructed directly from Java annotations. This enables topological rebalancing even on database clusters where physical foreign key constraints have been omitted for write performance.
5. **DAG Validation**: Fractal validates that the entity graph has a single root, contains no circular cycles, and ensures every descendant entity can reach the root via hops.
6. **Zero-Config Rebalancing**: When domain entity annotations are used, all YAML table properties (`root-table`, `root-id-column`, `status-column`, `sharded-tables`) can be omitted.

---

### Database Catalog Dependency Resolution (ANSI Information Schema)

When domain entity annotations are absent or when `fractal.sharding.rebalancer.shard-all: true` is enabled, [`TableDependencyResolver`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TableDependencyResolver.java) interrogates the ANSI database schema catalog:

1. **Catalog Introspection**: Queries `information_schema.referential_constraints` and `information_schema.key_column_usage`.
2. **Multi-Hop BFS Traversal**: Traces foreign key paths recursively from `rootTable` to discover all child tables.
3. **Kahn's Topological Sort**:
   - **Insert Ordering**: Root/parent tables first, moving downstream to leaf child tables.
   - **Delete Ordering**: Reverse of the insert order (leaf child tables first, root tables last).
4. **Catalog-Wide Sharding (`shard-all`)**:
   - Automatically inspects all base user tables in the database.
   - Excludes internal system schemas (`pg_catalog`, `information_schema`, `sys`) and tables declared in `exclude-tables`.
   - Tables with foreign key paths to `rootTable` are partitioned and topologically ordered for migration.
   - Tables with no path to `rootTable` are classified as **replicated reference tables** and synchronized across all shards.

---

### Rebalance Execution Lifecycle & Two-Phase State Machine

[`RebalanceEngine`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/RebalanceEngine.java) executes migration actions with two-phase progress tracking in `fractal_tenant_migrations`:

```sql
CREATE TABLE IF NOT EXISTS fractal_tenant_migrations (
    tenant_id VARCHAR(255) PRIMARY KEY,
    source_shard VARCHAR(255) NOT NULL,
    target_shard VARCHAR(255) NOT NULL,
    phase VARCHAR(50) NOT NULL,
    started_at TIMESTAMP NOT NULL
);
```

#### 8-Step Migration Execution Sequence:

```
[Start Migration for Tenant]
             |
             v
1. Lock Tenant & Drain Transactions
   - UPDATE root SET status = 'MIGRATING'
   - In-memory Caffeine cache set to true
   - Await in-flight requests (drain-timeout)
             |
             v
2. Record COPYING Phase
   - INSERT into fractal_tenant_migrations (phase = 'COPYING')
             |
             v
3. Idempotent Target Sanitization
   - Purge any partial rows on target shard in reverse order
             |
             v
4. Replicate Data
   - Batch copy rows in topological order (Parent -> Child)
   - Dynamic parameter bind chunking
             |
             v
5. Record PRUNING Phase
   - UPDATE fractal_tenant_migrations SET phase = 'PRUNING'
             |
             v
6. Evict Source Data
   - Batch delete rows from source shard in reverse order (Child -> Parent)
             |
             v
7. Clear Migration Record
   - DELETE from fractal_tenant_migrations
             |
             v
8. Unlock Tenant
   - UPDATE root SET status = 'ACTIVE'
   - In-memory Caffeine cache set to false
             |
             v
[Migration Complete]
```

1. **Lock Entity & Drain In-Flight Transactions**: Sets tenant status to `MIGRATING` on the master Primary table and primes the Caffeine cache. Waits up to `drain-timeout` (default: `10s`) for local transactions to drain to 0 via `awaitTenantQuiescence`. If transactions fail to drain, migration is deferred safely. An optional `quiescence-period` (default: `0s`) provides a cluster-wide pause for distributed transactions.
2. **Record Copy Phase**: Registers the tenant in `fractal_tenant_migrations` with `phase = 'COPYING'`.
3. **Idempotent Target Sanitization**: Checks if an earlier aborted migration left orphan rows on the target shard. If source still holds data, partial target rows are safely purged in reverse topological order.
4. **Data Replication**: Selects and streams rows from source to target in topological insert order using multi-hop SQL joins. Batch sizes are dynamically bounded:
   $$\text{chunkSize} = \min\left(\text{batchSize}, \left\lfloor \frac{\text{maxBatchParameters}}{\text{columnCount}} \right\rfloor\right)$$
   preventing JDBC bind parameter overflow (PostgreSQL's 65,535 limit or SQLite's 32,766 limit).
5. **Record Pruning Phase**: Sets `phase = 'PRUNING'` in `fractal_tenant_migrations`.
6. **Data Eviction**: Deletes migrated rows from source shard in reverse topological order.
7. **Clear Migration State**: Deletes row from `fractal_tenant_migrations`.
8. **Unlock Entity**: Restores tenant status to `ACTIVE` on Primary DB and updates cache.

---

### Idempotent Crash Recovery & Resumption Mechanics

If an application instance terminates mid-migration (SIGKILL, container preemption, power loss), the engine resumes safely upon restart:

| Failure Point | State on Disk | Recovery Action upon Restart |
| :--- | :--- | :--- |
| **Aborted during `COPYING`** | Target shard has partial data; source shard data is intact. | Source shard remains authoritative. The engine purges incomplete records from the target shard and restarts copying from scratch. |
| **Aborted during `PRUNING`** | Target shard has 100% of committed data; source shard has partial data. | The engine detects that target data is complete. It skips copying (preventing unique constraint errors) and finishes the remaining deletions on the source shard. |
| **Completed before crash** | Data exists only on target shard (`!sourceHasData && targetHasData`). | The engine marks tenant `ACTIVE`, removes stale tracking records, and completes immediately. |
| **Repetitive Execution** | Migration engine invoked repeatedly on identical state. | Yields zero side effects and deterministic completion. |

---

## 4. Use Cases & Reference Configurations

### Architecture Profile Matrix

| Architecture Profile | Sharding Key Source | Entity Discovery Strategy | Rebalancing & Migrations | Replica & Reference Tables | Ideal For |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Profile 1: B2B Multi-Tenant SaaS** | JWT Security Claim (`tenant_id`, `org_id`) | Domain Annotations ([`@ShardedEntity`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedEntity.java), [`@ShardedKey`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedKey.java), [`@ShardedStatus`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedStatus.java)) | Automated Rebalancer (`enabled: true`, zero-config) | [`@ShardedReplica`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedReplica.java) + [`@ShardedBroadcast`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedBroadcast.java) for lookup tables | Multi-tenant SaaS with authenticated enterprise tenants |
| **Profile 2: High-Throughput User / Account Partitioning** | Service Method SpEL (`#userId`, `#accountId`) | Domain Annotations or Database Catalog | Automated Rebalancer (`enabled: true`) | Replicated reference tables (`currencies`, `tiers`) | B2C E-Commerce, FinTech, social feeds, gaming platforms |
| **Profile 3: Turnkey Legacy Catalog Sharding** | JWT or Method SpEL | Database Catalog Introspection (`shard-all: true`) | Automated Rebalancer (`enabled: true`, topological sort) | Unconnected tables auto-replicated to all shards | Existing relational databases with established foreign key constraints |
| **Profile 4: Read-Mostly Reference Replication** | JWT or Method SpEL | Domain Entities with [`@ShardedReplica`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedReplica.java) | Optional | Startup sync via [`ReplicaTableSynchronizer`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/ReplicaTableSynchronizer.java) + parallel [`@ShardedBroadcast`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedBroadcast.java) | Global reference catalogs requiring local shard joins |
| **Profile 5: Asynchronous Worker / Event Consumer** | Method SpEL on Message Payload (`#event.tenantId`) | N/A (Stateless Routing) | Disabled (`rebalancer.enabled: false`) | Local caching or primary queries | Background jobs, Kafka/RabbitMQ consumers, batch workers |

---

### Profile 1: B2B Multi-Tenant SaaS (Zero-Config)

In B2B multi-tenant applications, authenticated REST requests carry a JWT with an enterprise tenant ID (`org_id`). Tenants require strict isolation across physical database shards.

```yaml
fractal:
  sharding:
    enabled: true
    virtual-nodes: 150
    jwt:
      claim-name: org_id
    primary:
      jdbc-url: jdbc:postgresql://db-primary:5432/saas_primary
      username: saas_admin
      password: ${DB_PASS}
    shards:
      shard-1:
        jdbc-url: jdbc:postgresql://db-shard1:5432/saas_shard1
        username: saas_user
        password: ${DB_PASS}
      shard-2:
        jdbc-url: jdbc:postgresql://db-shard2:5432/saas_shard2
        username: saas_user
        password: ${DB_PASS}
    rebalancer:
      enabled: true
      lock-timeout: 15m
      lock-refresh-interval: 1m
      drain-timeout: 10s
      status-cache-ttl: 2s
```

- **Domain Model**:
  ```java
  @Entity
  @Table(name = "organizations")
  @ShardedEntity(root = true)
  public class Organization {
      @Id @ShardedKey private String id;
      @ShardedStatus private String status;
  }
  ```
- **Service Layer**: Zero routing code required. Annotating service methods with [`@Sharded`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/Sharded.java) automatically extracts `org_id` from the active JWT in `SecurityContextHolder`.

---

### Profile 2: High-Throughput User / Account Partitioning (SpEL)

For B2C consumer platforms (such as consumer banking, crypto wallets, or mobile apps), requests are routed according to a specific user ID or account number supplied in method arguments.

```yaml
fractal:
  sharding:
    enabled: true
    virtual-nodes: 250   # Higher density for ultra-uniform ring distribution
    primary:
      jdbc-url: jdbc:postgresql://db-primary:5432/fintech_primary
      username: admin
      password: ${DB_PASS}
    shards:
      shard-alpha:
        jdbc-url: jdbc:postgresql://db-alpha:5432/fintech_shard_alpha
        username: app_user
        password: ${DB_PASS}
      shard-beta:
        jdbc-url: jdbc:postgresql://db-beta:5432/fintech_shard_beta
        username: app_user
        password: ${DB_PASS}
      shard-gamma:
        jdbc-url: jdbc:postgresql://db-gamma:5432/fintech_shard_gamma
        username: app_user
        password: ${DB_PASS}
    rebalancer:
      enabled: true
```

- **Service Layer**:
  ```java
  @Service
  public class AccountService {
      @Sharded(key = "#accountId")
      @Transactional
      public Account balanceTransfer(String accountId, BigDecimal amount) {
          // Routes directly to the shard owning accountId
          return accountRepository.updateBalance(accountId, amount);
      }
  }
  ```

---

### Profile 3: Turnkey Legacy Catalog Sharding (shard-all)

For existing monoliths with mature database schemas, Fractal can introspect the database schema catalog directly without modifying existing JPA entities.

```yaml
fractal:
  sharding:
    enabled: true
    primary:
      jdbc-url: jdbc:postgresql://db-primary:5432/legacy_primary
      username: dba
      password: ${DB_PASS}
    shards:
      shard-01:
        jdbc-url: jdbc:postgresql://db-shard01:5432/legacy_shard01
        username: app
        password: ${DB_PASS}
      shard-02:
        jdbc-url: jdbc:postgresql://db-shard02:5432/legacy_shard02
        username: app
        password: ${DB_PASS}
    rebalancer:
      enabled: true
      shard-all: true
      root-table: customers
      root-id-column: customer_id
      status-column: sync_status
      exclude-tables:
        - flyway_schema_history
        - audit_logs
```

- **Catalog Resolution**: Discovers foreign key constraints via ANSI `information_schema`. Tables with foreign key relations to `customers` are topologically ordered for sharded migrations. Tables without paths to `customers` are auto-replicated to all shards.

---

### Profile 4: Read-Mostly Reference Replication & Broadcasting

Designed for applications with heavy relational joins between sharded operational data (e.g. orders, line items) and reference data (e.g. currencies, product catalogs, tax rates).

```yaml
fractal:
  sharding:
    enabled: true
    primary:
      jdbc-url: jdbc:postgresql://db-primary:5432/shop_primary
      username: admin
      password: ${DB_PASS}
    shards:
      shard-1:
        jdbc-url: jdbc:postgresql://db-s1:5432/shop_shard1
        username: app
        password: ${DB_PASS}
      shard-2:
        jdbc-url: jdbc:postgresql://db-s2:5432/shop_shard2
        username: app
        password: ${DB_PASS}
    rebalancer:
      enabled: false  # Rebalancer optional; replica sync always runs on startup
```

- **Reference Entity**:
  ```java
  @Entity
  @Table(name = "currencies")
  @ShardedReplica
  public class Currency {
      @Id private String code;
      private BigDecimal rate;
  }
  ```
- **Broadcast Writes**:
  ```java
  @Service
  public class CurrencyAdminService {
      @ShardedBroadcast  // Concurrently writes updates to primary and all physical shards
      @Transactional
      public void updateRate(String code, BigDecimal newRate) {
          currencyRepository.updateRate(code, newRate);
      }
  }
  ```
- **Local Shard Join**:
  ```sql
  -- Native local SQL join on shard-1 with zero cross-network calls
  SELECT o.id, o.total * c.rate
  FROM orders o
  JOIN currencies c ON o.currency = c.code
  WHERE o.org_id = :orgId
  ```

---

### Profile 5: Asynchronous Worker / Event-Consumer Nodes

In clusters with dedicated worker pods (e.g., Kafka consumers, RabbitMQ listeners, `@Scheduled` tasks), nodes route asynchronous messages to the proper shard without participating in rebalancing coordination.

```yaml
fractal:
  sharding:
    enabled: true
    primary:
      jdbc-url: jdbc:postgresql://db-primary:5432/cluster_primary
      username: worker_user
      password: ${DB_PASS}
      initialize-schema: false  # Coordination tables managed by primary web API pods
    shards:
      shard-1:
        jdbc-url: jdbc:postgresql://db-shard1:5432/cluster_shard1
        username: worker_user
        password: ${DB_PASS}
      shard-2:
        jdbc-url: jdbc:postgresql://db-shard2:5432/cluster_shard2
        username: worker_user
        password: ${DB_PASS}
    rebalancer:
      enabled: false  # Workers must not trigger rebalance migrations
```

- **Consumer Method**:
  ```java
  @KafkaListener(topics = "order-events")
  @Sharded(key = "#event.tenantId")
  @Transactional
  public void processOrderEvent(OrderEvent event) {
      orderProcessor.handle(event);
  }
  ```

---

## 5. Configuration Property Specifications

### Comprehensive Property Matrix

All properties are rooted under `fractal.sharding`:

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
| `fractal.sharding.rebalancer.shard-all` | `boolean` | `false` | When `true`, automatically shards all database tables (catalog discovery) except excluded tables, ignoring `@ShardedEntity`. |
| `fractal.sharding.rebalancer.lock-timeout` | `Duration` | `15m` | Maximum lock expiration duration before an unreleased lock is considered dead and eligible for atomic takeover. |
| `fractal.sharding.rebalancer.lock-refresh-interval` | `Duration` | `1m` | Periodic heartbeat interval for renewing `locked_at` during an active rebalance migration. |
| `fractal.sharding.rebalancer.drain-timeout` | `Duration` | `10s` | Maximum duration to wait for pre-existing local in-flight transactions for a tenant to drain to 0 before deferring migration. Enforces minimum of `5s`. |
| `fractal.sharding.rebalancer.quiescence-period` | `Duration` | `0s` | Optional cluster-wide pause after setting `MIGRATING` status before copying data, giving remote nodes time to commit in-flight transactions. |
| `fractal.sharding.rebalancer.status-cache-ttl` | `Duration` | `2s` | Time-to-live for cached tenant migration status in local Caffeine in-memory cache to eliminate per-request DB queries. |
| `fractal.sharding.rebalancer.status-cache-max-size` | `long` | `50000` | Maximum number of tenant status entries cached in local memory (< 2MB RAM). |
| `fractal.sharding.rebalancer.batch-size` | `int` | `500` | Target chunk size for batch inserts during data copying and replica synchronization. |
| `fractal.sharding.rebalancer.max-batch-parameters` | `int` | `32766` | Maximum total JDBC bind parameters per chunk ($batchSize \times columns \le maxParameters$) to prevent parameter overflow errors (e.g. Postgres 65,535). |
| `fractal.sharding.rebalancer.root-table` | `String` | - | Master table holding tenant/entity records (e.g., `organizations`). Inferred from `@ShardedEntity(root = true)` if omitted. |
| `fractal.sharding.rebalancer.root-id-column` | `String` | - | Partition column name (e.g., `org_id`). Inferred from root `@ShardedKey` or `@Id` if omitted. |
| `fractal.sharding.rebalancer.status-column` | `String` | - | Column on `rootTable` indicating migration state. Inferred from root `@ShardedStatus` if omitted. |
| `fractal.sharding.rebalancer.migrating-value` | `String` | `MIGRATING` | State string set during an in-flight migration. Inferred from `@ShardedStatus(migratingValue = ...)` if omitted. |
| `fractal.sharding.rebalancer.active-value` | `String` | `ACTIVE` | State string when tenant is available. Inferred from `@ShardedStatus(activeValue = ...)` if omitted. |
| `fractal.sharding.rebalancer.sharded-tables` | `List<String>` | `null` | Optional explicit list of sharded tables. Discovered automatically from `@ShardedEntity` domain models or foreign key graph. |
| `fractal.sharding.rebalancer.replica-tables` | `List<String>` | `null` | Optional explicit list of reference tables to replicate across all shards. Inferred from `@ShardedReplica` or catalog when `shard-all: true`. |
| `fractal.sharding.rebalancer.exclude-tables` | `List<String>` | `null` | Optional list of tables to exclude from auto-discovery. |

---

### Exhaustive application.yml Reference

```yaml
fractal:
  sharding:
    enabled: true
    virtual-nodes: 150            # Virtual nodes per shard on the consistent hash ring
    jwt:
      claim-name: tenant_id       # JWT claim extracted for sharding key (default: 'sub')
    primary:
      jdbc-url: jdbc:postgresql://coordinator-db:5432/primary_meta
      username: fractal_admin
      password: ${PRIMARY_DB_PASSWORD}
      initialize-schema: true     # Auto-create coordination tables (fractal_locks, etc.)
    shards:
      shard-eu-1:
        jdbc-url: jdbc:postgresql://shard-eu-1:5432/shard_data
        username: app_user
        password: ${SHARD_EU1_PASSWORD}
      shard-eu-2:
        jdbc-url: jdbc:postgresql://shard-eu-2:5432/shard_data
        username: app_user
        password: ${SHARD_EU2_PASSWORD}
      shard-us-1:
        jdbc-url: jdbc:postgresql://shard-us-1:5432/shard_data
        username: app_user
        password: ${SHARD_US1_PASSWORD}
    rebalancer:
      enabled: true
      shard-all: false            # If true, auto-shards catalog; if false, uses @ShardedEntity
      lock-timeout: 15m           # Lock takeover threshold for dead node recovery
      lock-refresh-interval: 1m   # Heartbeat daemon interval to renew lock
      drain-timeout: 10s          # Timeout to drain local in-flight transactions (min 5s)
      quiescence-period: 0s       # Pause before data copy for distributed transaction drain
      status-cache-ttl: 2s        # Local Caffeine cache TTL for migration checks
      status-cache-max-size: 50000# Max entries in Caffeine cache
      batch-size: 500             # Rows per batch insert chunk
      max-batch-parameters: 32766 # Max bind parameters per chunk
      # When using @ShardedEntity, @ShardedKey, and @ShardedStatus, the properties below
      # are auto-discovered and do not need to be specified:
      # root-table: organizations
      # root-id-column: id
      # status-column: sync_status
      # migrating-value: MIGRATING
      # active-value: ACTIVE
      # sharded-tables: [organizations, projects, tasks]
      # replica-tables: [currencies, countries]
      exclude-tables:
        - flyway_schema_history
        - spatial_ref_sys
```

---

## 6. Advanced Usage & Integration Patterns

### Transparent Routing via Spring Security JWT

When `spring-boot-starter-oauth2-resource-server` is present on the classpath, [`JwtSecurityKeyExtractor`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/security/JwtSecurityKeyExtractor.java) is activated automatically:

- **Security Context Extraction**: Retrieves the authenticated `JwtAuthenticationToken` from `SecurityContextHolder.getContext().getAuthentication()`.
- **Claim Extraction**: Extracts the configured claim (`fractal.sharding.jwt.claim-name`, default: `sub`).
- **Type Coercion**: Handles String, numeric (Long/Integer), and UUID claim representations transparently.
- **Service Annotation**: Service methods can simply specify `@Sharded` without any parameters:
  ```java
  @Service
  public class InvoiceService {
      @Sharded
      @Transactional(readOnly = true)
      public List<Invoice> getRecentInvoices() {
          return invoiceRepository.findRecent();
      }
  }
  ```

---

### Handling Rebalance Migration Locks (TenantMigratingException)

When a tenant is actively migrating between shards, [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) rejects service requests by throwing [`TenantMigratingException`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/exception/TenantMigratingException.java). This prevents split-brain writes while data rows are in transit.

Handle this exception using a Spring `@RestControllerAdvice` to inform upstream API gateways or client applications:

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
                .body("Tenant " + ex.getTenantId() + " is currently migrating between database shards. Please retry shortly.");
    }
}
```

---

### Implementing a Custom ShardingKeyExtractor

To extract routing keys from custom protocols (e.g. HTTP headers, gRPC metadata, or cookies), implement [`ShardingKeyExtractor`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/core/ShardingKeyExtractor.java) and declare it as a Spring bean:

```java
package com.example.config;

import io.github.mucchinas.fractal.core.ShardingKeyExtractor;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class HeaderShardingKeyExtractor implements ShardingKeyExtractor {

    private static final String TENANT_HEADER = "X-Tenant-ID";

    @Override
    public String extractKey() {
        var attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes != null) {
            HttpServletRequest request = attributes.getRequest();
            String tenantId = request.getHeader(TENANT_HEADER);
            if (tenantId != null && !tenantId.isBlank()) {
                return tenantId.trim();
            }
        }
        return null;
    }
}
```

---

## 7. Technical Considerations, Pitfalls & Solutions

### Dual-Presence Schema Management & Flyway/Liquibase Best Practices

Fractal manages only its internal coordination tables (`fractal_shard_topology`, `fractal_locks`, `fractal_tenant_migrations`). Application domain tables must be managed consistently across databases.

#### 1. Primary Coordinator Database
- If `fractal.sharding.primary.initialize-schema: true` (default), Fractal auto-creates coordination tables at startup via ANSI DDL.
- In enterprise environments where applications lack DDL privileges at runtime, set `initialize-schema: false` and execute the provided DDL script (`src/main/resources/schema-primary.sql`) through Flyway/Liquibase.
- The Primary DB must also host the master root table (e.g. `organizations`) and master reference tables (e.g. `currencies`).

#### 2. Physical Shards
- Business domain tables must be provisioned identically across all physical shards.
- Configure Flyway or Liquibase to run migrations across all shard datasources during deployment:
  ```java
  @Bean
  public CommandLineRunner runMigrations(Map<String, DataSource> dataSources) {
      return args -> {
          dataSources.forEach((name, ds) -> {
              Flyway.configure()
                  .dataSource(ds)
                  .locations("classpath:db/migration")
                  .load()
                  .migrate();
          });
      };
  }
  ```

---

### Joining Sharded and Non-Sharded Data

Because sharded tables and primary tables reside in different physical database instances and connection pools, single SQL `JOIN` queries cannot be executed across them at the JDBC layer.

#### Pattern 1: Application-Level Join via Orchestrator Facade (Recommended)

```java
// 1. Sharded Service: executes queries on tenant's assigned shard
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

// 3. Orchestrator Facade: performs in-memory join
@Service
public class OrderReportingFacade {
    @Autowired private OrderService orderService;
    @Autowired private CurrencyService currencyService;

    // IMPORTANT: Do NOT annotate this orchestrator method with @Transactional.
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
> If the orchestrator facade method itself is marked `@Transactional`, Spring's transaction manager acquires a connection from the default target datasource (`primary`) *before* `@Sharded` evaluates. Child service calls then reuse the already-bound primary connection, causing table-not-found errors. Keep transactions on leaf service methods only.

#### Pattern 2: Broadcast / Replicated Tables (For High-Frequency Lookups)

1. Annotate domain entities with [`@ShardedReplica`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedReplica.java).
2. [`ReplicaTableSynchronizer`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/ReplicaTableSynchronizer.java) copies reference tables from the primary DB to all shards at application startup and whenever new shards join.
3. Annotate mutation methods with [`@ShardedBroadcast`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedBroadcast.java) to execute updates across primary and all shards in parallel.
4. Repositories can execute native, zero-latency local SQL joins between sharded and replicated tables directly on the shard.

#### Pattern 3: In-Memory / Distributed Caching

Cache global reference data in Redis or local memory using Spring Cache (`@Cacheable`). Sharded services query only their assigned shard and enrich DTOs with cached reference values.

---

### The Transaction Aggregation Problem and Propagation.REQUIRES_NEW Caveats

Coordinating multiple datasources in Spring introduces fundamental transaction management challenges:

#### The Transaction Aggregation Problem (Routing Lock-In)
1. **Single Connection Affinity**: Spring binds exactly one JDBC `Connection` per `@Transactional` block via `TransactionSynchronizationManager`.
2. **Early Connection Acquisition**: When a parent method declares `@Transactional`, Spring eagerly acquires a connection from `ShardingRoutingDataSource` before child `@Sharded` annotations are evaluated. With no shard set, `determineCurrentLookupKey()` returns `null`, locking the entire transaction onto `primary`.
3. **Silent Target Misdirection**: Subsequent calls to child `@Sharded` methods update `ShardContextHolder`, but Spring never calls `determineCurrentLookupKey()` again because a connection is already bound (`Propagation.REQUIRED`). Queries intended for a shard execute silently against the `primary` database.

#### The Pitfalls and Dangers of `Propagation.REQUIRES_NEW`
Attempting to bypass routing lock-in using `@Transactional(propagation = Propagation.REQUIRES_NEW)` introduces severe architectural hazards:

1. **Atomicity Breakdown & Lack of Distributed Rollback (Partial Failures)**:
   - `Propagation.REQUIRES_NEW` creates independent, autonomous physical database transactions.
   - If the inner transaction on the shard commits, but the outer transaction subsequently fails, **the inner transaction cannot be rolled back**.
   - Without an XA/2PC coordinator, this creates split-brain state and permanent data corruption.
2. **Connection Pool Starvation and Application Deadlock**:
   - A thread holding an outer connection while requesting an inner connection holds **multiple physical database connections simultaneously**.
   - Under concurrent load, all worker threads can acquire outer connections, exhausting pool capacity so none can acquire inner connections. This produces a **classic connection pool deadlock**, freezing the application.
3. **Isolation Anomalies and Dirty Reads**:
   - The inner transaction cannot observe uncommitted writes from the outer transaction.
   - Once committed, inner modifications become visible system-wide even if the outer workflow aborts.

#### Recommended Architectural Best Practices:
- **For Reads**: Omit `@Transactional` on the orchestrator facade. Place `@Transactional(readOnly = true)` strictly on leaf services.
- **For Multi-Shard Writes**:
  - Never use nested `REQUIRES_NEW` for cross-database atomicity.
  - Use the **Saga Pattern** with compensating actions for multi-step distributed workflows.
  - Use the **Transactional Outbox Pattern** to write business mutations and events to the shard in a single local transaction, publishing to other databases asynchronously via Kafka/RabbitMQ.

---

### Database Dialect Constraints & ANSI SQL Compatibility

- **ANSI Schema Introspection**: [`TableDependencyResolver`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TableDependencyResolver.java) uses standard ANSI `information_schema.referential_constraints` and `information_schema.key_column_usage` views, compatible with PostgreSQL, H2, MySQL, and modern relational engines.
- **Cross-Shard Queries**: Fractal is an application-level routing starter. SQL joins spanning multiple physical shards are not supported at the JDBC layer.
- **Unpartitioned Entities**: Entities accessed outside `@Sharded` methods route to the `primary` datasource by default.

---

## 8. Verification, Testing & Test Suite Reference

### Prerequisites
- Java Development Kit (JDK) 17+
- Apache Maven 3.8+

### Test Execution
```bash
mvn clean test
```

The test suite validates the starter across 16 test classes covering 59 automated test cases:

- `ConsistentHashRouterTest`: Validates deterministic routing, MD5 hash calculation, 64-bit ring wrap-around, and uniform distribution across virtual nodes.
- `RoutingIntegrationTest`: Tests dynamic shard switching, primary fallback, and method-level SpEL resolution using H2 databases.
- `SecurityRoutingIntegrationTest`: Confirms end-to-end shard routing from synthetic JWT tokens (`sub` claim) in `SecurityContextHolder`.
- `CustomClaimSecurityRoutingIntegrationTest`: Tests end-to-end shard routing using custom JWT claims (e.g. `tenant_id`).
- `JwtSecurityKeyExtractorTest`: Validates claim extraction, fallback logic, string/numeric/UUID claim conversions, and unauthenticated handling.
- `ShardingAspectMigrationLockTest`: Asserts that `TenantMigratingException` is thrown when accessing a tenant currently migrating.
- `TopologyManagerCaffeineCacheTest`: Validates sub-microsecond Caffeine caching, zero DB queries within TTL, cache invalidation on status updates, and automatic reload on TTL expiration.
- `TableDependencyResolverTest`: Verifies ANSI `information_schema` foreign key discovery, multi-hop BFS dependency resolution, Kahn's topological sort for insert/delete ordering, join query synthesis, table exclusion, and replica table isolation.
- `EntityTableMetadataResolverTest`: Validates domain entity auto-discovery via `@ShardedEntity`, `@ShardedKey`, `@ShardedStatus`, and `@ShardedReplica`, multi-tier hierarchy resolution, JPA `@Table`/`@JoinColumn`/`@Column`/`@Id` metadata extraction, custom status values, cycle detection, reachability checks, single-root enforcement, duplicate status prevention, and non-root status prohibition.
- `EntityRebalanceIntegrationTest`: Confirms end-to-end multi-hop tenant migration on physical databases without database foreign key constraints using entity-discovered plans and `@ShardedStatus`, while ensuring replicated tables (`currencies`) are preserved on all shards.
- `RebalanceEngineTest`: Validates end-to-end tenant migration, row copying across shards using multi-hop plans, and reverse-order row pruning.
- `ReplicaTableSynchronizerTest`: Tests batch synchronization of reference tables from primary coordinator to shards and new shard catch-up.
- `ShardedBroadcastAspectTest`: Confirms parallel broadcast writes across primary database and all physical shards for `@ShardedBroadcast` service methods.
- `TopologyManagerLockTest`: Verifies distributed lock acquisition, mutual exclusion, expired lock takeover via configurable TTL, periodic heartbeat renewal, graceful shutdown lock release, automatic schema initialization via `InitializingBean`, and idempotent ANSI shard registration.
- `RebalanceEngineIdempotencyTest`: Asserts idempotent crash recovery across migration phases, resumption from aborted `COPYING` state (purging partial target data and recopying), resumption from aborted `PRUNING` state (safe source pruning without duplicate inserts), and safe repeated execution without side effects.
- `QuiescenceAndBatchLimitTest`: Asserts in-flight request tracking, active execution draining via `awaitTenantQuiescence`, safe migration deferral on drain timeouts, cluster quiescence pauses, and dynamic chunk size calculation preventing bind parameter overflow on wide tables.
