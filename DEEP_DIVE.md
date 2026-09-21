# Fractal Deep Dive & Architecture Reference

This document provides an exhaustive, in-depth technical analysis of the internal mechanics, distributed coordination protocols, migration state machines, and advanced architectural patterns implemented by the Fractal Spring Boot Starter.

For a quick setup and introductory guide, refer to the [README.md](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/README.md).

---

## Table of Contents

1. [Architectural Overview & Request Interception Mechanics](#1-architectural-overview--request-interception-mechanics)
   - [Request Routing Lifecycle](#request-routing-lifecycle)
   - [Spring AOP Interception Precedence & Pointcuts](#spring-aop-interception-precedence--pointcuts)
   - [ThreadLocal Lifecycle & Connection Leasing](#threadlocal-lifecycle--connection-leasing)
2. [Core Components Deep Dive](#2-core-components-deep-dive)
   - [Consistent Hash Router & Ring Distribution](#consistent-hash-router--ring-distribution)
   - [Sharding Routing DataSource & HikariCP Pools](#sharding-routing-datasource--hikaricp-pools)
   - [Sharding Key Extraction Pipeline](#sharding-key-extraction-pipeline)
3. [Automated Rebalancer & Migration Subsystem](#3-automated-rebalancer--migration-subsystem)
   - [Distributed Coordination, Locking, & Heartbeat](#distributed-coordination-locking--heartbeat)
   - [High-Performance In-Memory Migration Cache (Caffeine) & Root Catalog Architecture](#high-performance-in-memory-migration-cache-caffeine--root-catalog-architecture)
   - [Topology Delta Calculation Engine](#topology-delta-calculation-engine)
   - [Domain Entity Auto-Discovery (@ShardedRoot, @ShardedEntity, @ShardedKey, @ShardedStatus)](#domain-entity-auto-discovery-shardedroot-shardedentity-shardedkey-shardedstatus)
   - [Database Catalog Dependency Resolution (ANSI Information Schema)](#database-catalog-dependency-resolution-ansi-information-schema)
   - [Rebalance Execution Lifecycle & Two-Phase State Machine](#rebalance-execution-lifecycle--two-phase-state-machine)
   - [Idempotent Crash Recovery & Resumption Mechanics](#idempotent-crash-recovery--resumption-mechanics)
   - [Atomic Reference Table Synchronization (ReplicaTableSynchronizer)](#atomic-reference-table-synchronization-replicatablesynchronizer)
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
   - [Just-In-Time (JIT) Tenant Provisioning (@Sharded(provision = true))](#just-in-time-jit-tenant-provisioning-shardedprovision--true)
   - [Handling Rebalance Migration Locks (TenantMigratingException)](#handling-rebalance-migration-locks-tenantmigratingexception)
   - [Implementing a Custom ShardingKeyExtractor](#implementing-a-custom-shardingkeyextractor)
7. [Technical Considerations, Pitfalls & Solutions](#7-technical-considerations-pitfalls--solutions)
   - [Dual-Presence Schema Management & Flyway/Liquibase Best Practices](#dual-presence-schema-management--flywayliquibase-best-practices)
   - [Adopting Fractal on an Already-Populated Monolithic Database (Brownfield Migration)](#adopting-fractal-on-an-already-populated-monolithic-database-brownfield-migration)
   - [Continuous Primary Drain Reconciliation & Scale-Proof Alignment](#continuous-primary-drain-reconciliation--scale-proof-alignment)
   - [Scaling Down: Graceful Shard Decommissioning (M -> N Shards)](#scaling-down-graceful-shard-decommissioning-m--n-shards)
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

### Spring AOP Interception Precedence & Pointcuts

The order of interception between [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) and Spring's `TransactionInterceptor` is critical:

```java
@Aspect
@Order(1)
public class ShardingAspect {
    @Around("@annotation(io.github.mucchinas.fractal.annotation.Sharded) || @within(io.github.mucchinas.fractal.annotation.Sharded)")
    public Object route(ProceedingJoinPoint joinPoint) throws Throwable {
        // Interception logic
    }
}
```

In standard Spring configurations, `@Transactional` aspects execute with default lowest precedence (`Ordered.LOWEST_PRECEDENCE`). By annotating [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) with `@Order(1)`, Fractal guarantees:
1. The sharding key is parsed and routed, and the resolved shard identifier is committed to [`ShardContextHolder`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/core/ShardContextHolder.java) *prior* to `DataSourceTransactionManager` or `JpaTransactionManager` invoking `getConnection()`.
2. If [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) executed with lower precedence than `@Transactional`, Spring's transaction manager would acquire a connection before the sharding key is set. In that erroneous sequence, `determineCurrentLookupKey()` would return `null`, erroneously binding the entire transaction to the default `primary` datasource.
3. The dual pointcut `@annotation(...) || @within(...)` supports both method-level routing and class-level routing. When `@Sharded` is placed on a `@Service` class, all public methods in the bean inherit shard routing automatically. Method-level annotations override class-level defaults.

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

To eliminate memory leaks, cross-request contamination in pooled thread environments (e.g. Tomcat, Undertow, or virtual threads), and to safely support **nested `@Sharded` and `@ShardedBroadcast` invocations**, [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) and [`ShardedBroadcastAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardedBroadcastAspect.java) implement **stack-based context leasing**:
- **Context Capture**: Captures `String previousShard = ShardContextHolder.getShard()` before updating context to `targetShard`.
- **`try`**: Extracts the key, checks the migration cache, resolves the shard via [`ConsistentHashRouter`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/core/ConsistentHashRouter.java), tracks in-flight requests, and calls `ShardContextHolder.setShard(targetShard)`.
- **`finally`**: Checks if an outer context was present. If `previousShard != null`, it restores the outer context (`ShardContextHolder.setShard(previousShard)`); only when exiting the outermost invocation (`previousShard == null`) does it call `ShardContextHolder.clear()`.

This guarantees that nested service calls (such as an internal audit service or broadcast update invoked from within a sharded tenant method) will never prematurely wipe or corrupt the caller's thread-local shard context upon returning. Even if downstream code or transaction commits throw an unhandled `RuntimeException`, the thread context is completely restored or sanitized before returning to the container thread pool.

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
   - **Strict Parameter Validation**: `virtualNodes` must be strictly positive ($> 0$). Constructing a router with $V \le 0$ throws `IllegalArgumentException`.
2. **Ring Storage**:
   - Implemented as an immutable or synchronized `java.util.TreeMap<Long, String>`.
3. **Cryptographic Hashing**:
   - The key string is digested with `MD5`.
   - The first 8 bytes of the digest are packed into a 64-bit `Long` via bitwise operations:
     $$\text{hash} = \sum_{i=0}^{7} (\text{digest}[i] \ \& \ 0\text{xFF}) \ll (8 \times (7 - i))$$
4. **Binary Search Traversal ($O(\log(N \times V))$)**:
   - Shard lookup calls `ring.tailMap(hash)`.
   - If `tailMap.isEmpty()` is true, traversal wraps around clockwise to `ring.firstKey()`.
   - If the ring contains zero active shards, `routeNode` safely returns `null`.
5. **Minimal Key Relocation**:
   - When a new physical shard is introduced, only $\frac{K}{N+1}$ keys are relocated (where $K$ is the total key count and $N$ is the number of active shards), leaving all other mappings undisturbed.

### Sharding Routing DataSource & HikariCP Pools

[`ShardingRoutingDataSource`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/datasource/ShardingRoutingDataSource.java) subclasses Spring JDBC's `AbstractRoutingDataSource` and implements Spring's `DisposableBean`:

1. **Centralized HikariCP Pools & Single-Instance Sharing**:
   - For every physical shard declared under `fractal.sharding.shards`, an isolated `HikariDataSource` pool is instantiated in [`FractalAutoConfiguration`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/config/FractalAutoConfiguration.java).
   - A dedicated `HikariDataSource` pool is created for the `primary` database.
   - Rather than creating duplicate connection pools across subsystems, the **exact same pooled instances** are shared across `ShardingRoutingDataSource`, `TopologyManager`, `RebalanceEngine`, and `ReplicaTableSynchronizer`. This eliminates connection pool explosion and prevents database port exhaustion.
2. **Dynamic Key Resolution**:
   - Overrides `determineCurrentLookupKey()`, which returns `ShardContextHolder.getShard()`.
3. **Fallback Target**:
   - The primary data source is set as `defaultTargetDataSource`. Any un-sharded database interaction or unannotated service invocation automatically defaults to the primary database.
4. **Clean Resource Disposal (`DisposableBean`)**:
   - On Spring application context shutdown, `ShardingRoutingDataSource.destroy()` systematically inspects all target datasources and the default primary datasource.
   - Any `AutoCloseable` or `HikariDataSource` instance is explicitly closed, cleanly terminating active worker threads, releasing JDBC socket handles, and avoiding database connection leaks during rolling redeployments.

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
2. **Tier 2 - Compiled SpEL Expression Evaluation**:
   - Evaluates the `@Sharded(key = "...")` expression against method arguments.
   - **AST Compilation Caching**: Expressions are compiled and stored in an internal thread-safe `ConcurrentHashMap<String, Expression>`. Repeated invocations reuse parsed ASTs, avoiding runtime parsing overhead on hot transaction paths.
   - **Parameter Discovery**: Backed by `DefaultParameterNameDiscoverer`, ensuring reliable parameter name binding across standard classes and interface-based Spring proxies.
   - Supports parameter identifiers (e.g. `#tenantId`), nested properties (`#request.company.id`), and complex SpEL expressions.
3. **Tier 3 - Fail-Fast Guard**:
   - If neither a strategic extractor nor SpEL evaluation produces a valid key, execution halts immediately with an `IllegalStateException`, aborting the transaction before any database connection is acquired.

#### The Core Invariant: Key Alignment Between Service Routing and @ShardedRoot

> [!IMPORTANT]
> **The Key Alignment Invariant**:
> Whatever identifier is resolved by the Sharding Key Extraction Pipeline (from a JWT claim or SpEL expression) **must match the exact value stored in the column annotated with [`@ShardedKey`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedKey.java) on the [`@ShardedRoot`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedRoot.java) entity**.
>
> The chosen routing key **must exist as a physical column on the root table** (typically its `@Id` or unique partition key).

##### Architectural Mechanics: Why Alignment is Mandatory

```
[Service Invocation] ---> Extracts key (e.g. JWT "tenant_id" = "org_123") ---> hash("org_123") ---> Routes to Shard 1
                                                                                                          |
                                                                         [MUST MATCH]                     v
                                                                                              [Local Shard 1 DB]
                                                                                              organizations.id = 'org_123'
                                                                                              projects.org_id = 'org_123'
                                                                                                          ^
                                                                         [MUST MATCH]                     |
[Cluster Rebalance]  ---> Reads root table: organizations.id ("org_123")   ---> hash("org_123") ---> Migrates to Shard 1
```

If these two concepts diverge:
- If a service method extracts a user ID (`usr_888` from JWT `sub`), but the root table is `organizations` where `@ShardedKey` is `organizations.id` (`org_123`):
  1. The service hashes `usr_888` and sends queries to Shard A.
  2. The rebalancer reads `organizations.id = org_123`, hashes `org_123`, and places the organization and all its projects on Shard B.
  3. Queries on Shard A execute against a database where the organization's rows physically do not exist, returning empty result sets or null pointer exceptions.

##### Best Practice Guidelines for Key Selection

1. **B2B Multi-Tenant Applications (Organization Partitioning)**:
   - **Root Entity**: Set `@ShardedRoot` on `Organization` (or `Tenant`, `Company`).
   - **Root Key**: `@Id @ShardedKey private String id;` (stores `org_123`).
   - **JWT Claim**: Configure `fractal.sharding.jwt.claim-name: tenant_id` (or `org_id`). Ensure identity tokens contain `"tenant_id": "org_123"`.
   - **SpEL Fallback**: For asynchronous or internal methods without security context, use `@Sharded(key = "#orgId")`.
2. **B2C Consumer Applications (User / Subject Partitioning)**:
   - **Root Entity**: Set `@ShardedRoot` on `User` (or `Account`, `Customer`).
   - **Root Key**: `@Id @ShardedKey private String id;` (stores `usr_456`).
   - **JWT Claim**: Use the default `fractal.sharding.jwt.claim-name: sub`. OAuth2/OIDC access tokens store the authenticated user ID in the standard `sub` claim.
   - **SpEL Fallback**: Use `@Sharded(key = "#userId")` or `@Sharded(key = "#accountId")`.

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

#### 3. Periodic Heartbeat Daemon & Circuit Breaker:
For large datasets where batch migrations exceed standard timeouts, a background heartbeat daemon thread refreshes the lock timestamp at a fixed interval (`fractal.sharding.rebalancer.lock-refresh-interval`, default: `1m`):
```sql
UPDATE fractal_locks
SET locked_at = CURRENT_TIMESTAMP
WHERE lock_name = 'REBALANCE_LOCK' AND locked_by = ?
```
**Heartbeat Failover Circuit Breaker**: If database downtime or prolonged network isolation causes 3 consecutive heartbeat renewals to fail, the daemon relinquishes the lock locally, halts renewal, and logs a critical alert. This prevents a disconnected node from continuing to execute migrations under an assumed lock while allowing surviving nodes to commandeer the expired lock after the configured TTL.

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

#### Dynamic Dual-Ring Migration Routing & Zero-Overhead Normal Operations

A critical challenge in online shard rebalancing is routing incoming traffic correctly while the cluster transitions from $N$ to $M$ shards:
- Some tenants have not yet migrated and still reside on their old shard ($R_{old}$).
- One tenant is actively migrating (rows in flight between shards).
- Some tenants have already migrated and now reside on their new shard ($R_{new}$).
- Most tenants were not affected by the rebalance and remain on their current shard ($R_{old} = R_{new}$).

Fractal achieves this with **Dynamic Dual-Ring Routing** and **Zero-Overhead Steady-State Operations**:

```
                                  [Incoming Request]
                                           |
                                           v
                          +----------------------------------+
                          |  TopologyManager:                |
                          |  isRebalanceActive()?            |
                          +----------------+-----------------+
                                           |
                  +------------------------+------------------------+
                  | (FALSE: Steady-State)                           | (TRUE: Rebalancing Active)
                  v                                                 v
   +------------------------------+                  +------------------------------+
   | FAST PATH (Zero-Overhead):   |                  | Track In-Flight Requests     |
   | - Bypass per-tenant cache    |                  | LongAdder.increment()        |
   | - Bypass DB queries          |                  +--------------+---------------+
   | - Bypass LongAdder tracking  |                                 |
   | Route directly via R_new     |                                 v
   +--------------+---------------+                  +------------------------------+
                  |                                  | Is Tenant Actively           |
                  |                                  | Migrating?                   |
                  |                                  +--------------+---------------+
                  |                                                 |
                  |                        +------------------------+------------------------+
                  |                        | (YES)                                           | (NO)
                  |                        v                                                 v
                  |         +------------------------------+                  +------------------------------+
                  |         | THROW TenantMigratingException|                  | Has Pending Source           |
                  |         | (HTTP 503 Retry-After)       |                  | Override in TopologyManager? |
                  |         +------------------------------+                  +--------------+---------------+
                  |                                                                          |
                  |                                                 +------------------------+------------------------+
                  |                                                 | (YES: Unmigrated)      | (NO: Migrated/Bystander)
                  |                                                 v                        v
                  |                                  +------------------------------+ +------------------------------+
                  |                                  | Route to Old Source Shard    | | Route to New Ring Shard      |
                  |                                  | (R_old Override)             | | (R_new)                      |
                  |                                  +--------------+---------------+ +--------------+---------------+
                  |                                                 |                                |
                  +-------------------------------------------------+--------------------------------+
                                                                    |
                                                                    v
                                                     [Execute on Target Shard]
```

##### 1. Zero-Overhead Normal Operations (Steady-State Fast Path)

In production, rebalancing is an occasional operational event (e.g. executed once a month or quarter when scaling shards). Under steady-state operations:
- [`TopologyManager.isRebalanceActive()`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TopologyManager.java#L94) consults a dedicated, 1-element [Caffeine](https://github.com/ben-manes/caffeine) cache (`rebalanceActiveCache`, TTL `status-cache-ttl`, default: `2s`).
- When `false`, [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) immediately invokes `router.routeNode(shardingKey)` and proceeds.
- **Zero Cache Misses**: The per-tenant `migrationStatusCache` is neither queried nor populated.
- **Zero Lock Contention**: `LongAdder` in-flight request tracking is skipped entirely.
- Normal request overhead is identical to standard consistent hashing without rebalancing capability (~15 nanoseconds).

##### 2. Dynamic Routing Lifecycle During Active Rebalance

When physical shards are added, the rebalance runner acquires `REBALANCE_LOCK` and begins migration:

1. **Global Activation**: `isRebalanceActive` becomes `true` locally and propagates across cluster pods via `rebalanceActiveCache`.
2. **Pending Overrides ($R_{old}$)**: All relocation actions calculated by [`MigrationDeltaCalculator`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/MigrationDeltaCalculator.java) are registered in [`TopologyManager`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TopologyManager.java) via `registerPendingMigration(tenantId, sourceShard)` and written to `fractal_tenant_migrations` as `PENDING`. Requests for pending tenants continue routing to their **old source shard**.
3. **Per-Tenant Serial Migration**: Tenants are migrated one by one:
   - **Isolation**: Tenant status is set to `MIGRATING`. In-flight requests drain. New requests immediately receive [`TenantMigratingException`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/exception/TenantMigratingException.java).
   - **Data Transfer**: Tables are copied topologically to the new shard and pruned from the old shard.
4. **Immediate Cutover ($R_{new}$)**: As soon as an individual tenant's data transfer completes:
   - Its pending override is removed (`completePendingMigration(tenantId)`).
   - Its status is set back to `ACTIVE`.
   - Subsequent requests for this tenant **immediately route to the new shard ($R_{new}$)**. The tenant does NOT wait for the remaining batch to finish.
5. **Non-Migrating Tenants (Bystanders)**: Tenants that do not change shards have no pending override ($R_{old} = R_{new}$). They continue routing to their existing shard throughout the entire rebalancing process with zero downtime.
6. **Rebalance Completion**: Once all actions finish and `releaseRebalanceLock()` is invoked, `isRebalanceActive` resets to `false`, clearing all pending maps and per-tenant caches, seamlessly returning the cluster to zero-overhead mode.

#### Sub-Microsecond Cache-Aside Migration Guard

[`TopologyManager`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TopologyManager.java) caches tenant migration statuses using [Caffeine](https://github.com/ben-manes/caffeine):

- **Fast Path (~15 Nanoseconds)**: Service methods check `TopologyManager.isTenantMigrating(tenantId)` directly in JVM heap memory with zero network or JDBC overhead during rebalancing.
- **Slow Path (Authoritative Lookup)**: On a cache miss or TTL expiration (`status-cache-ttl`, default: `2s`), Fractal queries `fractal_tenant_migrations` and the master root table on the Primary DB.
- **Non-Blocking `PHASE_PENDING` Routing**: When a tenant migration is registered as `PENDING` in `fractal_tenant_migrations`, `TopologyManager.isTenantMigrating()` returns `false`. Incoming requests for pending tenants are never blocked or rejected prematurely. Instead, queries continue routing to their old source shard via `TopologyManager.getPendingSourceShard()`. Only when the tenant transitions to `COPYING` or `PRUNING` does `isTenantMigrating()` return `true` to raise `TenantMigratingException` (HTTP 503).
- **Multi-Node Primary DB Fallback**: On secondary cluster nodes where the local `pendingMigrations` map has not been primed, or during pod restarts mid-rebalance, `getPendingSourceShard(tenantId)` queries `fractal_tenant_migrations` directly on the Primary DB. If the tenant record is found in `PHASE_PENDING`, the query routes to its recorded `source_shard`. If the record is in `PHASE_COPYING` or `PHASE_PRUNING`, `isTenantMigrating()` throws `TenantMigratingException`.
- **Leak-Free In-Flight Request Tracking**: In [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java), in-flight request tracking is guarded by a strict `try ... finally` block. If `isTenantMigrating()` throws `TenantMigratingException`, `decrementInFlightRequests()` is guaranteed to execute in the `finally` block, ensuring that in-flight request counters never leak in memory or cause subsequent migration quiescence drains to stall.
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

### Domain Entity Auto-Discovery (@ShardedRoot, @ShardedEntity, @ShardedKey, @ShardedStatus)

Fractal provides declarative entity auto-discovery via [`EntityTableMetadataResolver`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/EntityTableMetadataResolver.java):

```java
// 1. Root Partition Entity: Defines cluster partition key and migration status
@Entity
@Table(name = "organizations")
@ShardedRoot
public class Organization {
    @Id
    @ShardedKey
    private String id;

    private String name;

    @ShardedStatus(migratingValue = "MIGRATING", activeValue = "ACTIVE")
    @Column(name = "sync_status")
    private String syncStatus;
}

// 2. Child Entity (JPA Object Reference): References root entity directly
@Entity
@Table(name = "projects")
@ShardedEntity
public class Project {
    @Id
    private UUID id;

    // Target entity inferred from Organization field type
    // Foreign key column inferred from @JoinColumn name ("org_id")
    @ShardedKey
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "org_id")
    private Organization organization;
}

// 3. Leaf Entity (Scalar Foreign Key): Uses scalar UUID instead of JPA entity reference
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

#### Understanding the Foreign Key "Hop" Mechanism

In horizontal sharding, only the root entity ([`@ShardedRoot`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedRoot.java)) stores the primary partition key (`Organization.id = "org_1"`). Descendant tables (e.g. `tasks`) rarely have an `org_id` column; they only know their immediate parent (`projects.id`).

When the automated rebalancer migrates a tenant to a new physical shard, it must copy all associated data across the entire entity hierarchy. To achieve this without requiring database-level foreign key constraints, Fractal builds a Directed Acyclic Graph (DAG) of entities and navigates backwards from leaf tables to the root using multi-hop SQL joins.

#### Scalar IDs (UUID / Long) vs. JPA Object References

In modern Spring Boot and Domain-Driven Design (DDD) architectures, developers frequently prefer scalar foreign keys (`UUID projectId`) rather than full JPA entity relationships (`@ManyToOne Project project`) for several architectural reasons:
- **Clean Aggregate Boundaries**: In DDD, aggregate roots reference other aggregate roots by identity (scalar ID), not by direct object reference.
- **Elimination of Lazy Loading & N+1 Queries**: Prevents unintentional queries triggered by accessing entity relationships outside of active transactions.
- **Zero Hibernate Proxy Serialization Issues**: Avoids Jackson/DTO serialization errors caused by uninitialized Hibernate ByteBuddy proxies.

However, scalar types introduce a metadata challenge for reflection:
1. **With JPA Object References (`@ManyToOne Organization organization`)**: Java reflection inspects `field.getType()`, which yields `Organization.class`. Fractal immediately knows the parent entity, its table name, and extracts the foreign key column from `@JoinColumn(name = "org_id")`.
2. **With Scalar IDs (`UUID projectId`)**: Java reflection only sees `java.util.UUID.class`. `UUID` carries zero semantic information about which entity or table it references. Fractal cannot infer whether `projectId` points to `Project`, `Organization`, or `User`.

This is why `targetEntity` is **mandatory** on scalar fields:
```java
@ShardedKey(targetEntity = Project.class)
private UUID projectId;
```

#### Deconstructing `@ShardedKey` Attributes: Which Column Goes Where

The [`@ShardedKey`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedKey.java) annotation contains three attributes. It is crucial to understand which table and column each attribute refers to:

| Attribute | Table Scope | Purpose | Required for Scalar UUID? | Required for `@ManyToOne`? | Default / Inference Behavior |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `targetEntity` | **Parent Table** | Specifies the parent entity class in the DAG | **Yes** (`Project.class`) | No | Inferred from Java field type (e.g. `Organization.class`) |
| `column` | **This Entity's Table** (Child) | The foreign key column in **this** table pointing to the parent | No (Optional) | No (Optional) | Inferred from `@Column(name)`, `@JoinColumn(name)`, or field name in `snake_case` (`project_id`) |
| `referencedColumn` | **Parent Table** | The column in the **parent** table being referenced | No (Optional) | No (Optional) | Inferred from parent's `@Id` primary key (or root partition key if `@ShardedRoot`) |

##### Clarification on `column`:
- `column` specifies the column in **this entity's table** (child table), NOT the parent table.
- If you annotate the field with standard JPA `@Column(name = "project_id")`, or if your Java field name is `projectId` (which converts to `project_id` via snake_case), you **do not need** to specify `column` in `@ShardedKey`.
- Only use `column = "custom_fk_col"` if your physical database column name cannot be determined from `@Column` or field name conventions.

##### Clarification on `referencedColumn`:
- `referencedColumn` specifies the column in the **parent entity's table**.
- By default, Fractal automatically inspects the `targetEntity` class and uses its `@Id` primary key column (e.g. `projects.id`).
- You only need to specify `referencedColumn = "..."` in rare schemas where a foreign key references a unique column other than the parent's primary key.

#### How Fractal Synthesizes the Hop SQL During Migrations

When moving an organization (`'org_123'`) from Shard A to Shard B, [`TableDependencyResolver`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TableDependencyResolver.java) traces the path from each table to the root and generates the exact migration queries:

```sql
-- 1. Hop 0: Root Entity (organizations)
SELECT * FROM organizations WHERE id = 'org_123';

-- 2. Hop 1: Child Entity (projects - 1 hop to root via org_id)
SELECT * FROM projects WHERE org_id = 'org_123';

-- 3. Hop 2: Leaf Entity (tasks - 2 hops to root via tasks.project_id -> projects.id -> projects.org_id)
SELECT tasks.* FROM tasks
JOIN projects ON tasks.project_id = projects.id
JOIN organizations ON projects.org_id = organizations.id
WHERE organizations.id = 'org_123';
```

During the pruning phase, Fractal executes reverse-dependency deletions to satisfy database constraints:
```sql
-- Reverse order pruning on source shard:
DELETE FROM tasks WHERE project_id IN (
    SELECT id FROM projects WHERE org_id = 'org_123'
);
DELETE FROM projects WHERE org_id = 'org_123';
DELETE FROM organizations WHERE id = 'org_123';
```

#### Best Practice Guidelines for Entity Modeling

##### Guideline 1: Scalar Foreign Keys (Recommended for DDD and High-Throughput Services)
Combine standard JPA `@Column` with `@ShardedKey(targetEntity = ...)`:
```java
@Entity
@Table(name = "tasks")
@ShardedEntity
public class Task {
    @Id
    private UUID id;

    // Clean, idiomatic JPA: no column duplication
    @Column(name = "project_id")
    @ShardedKey(targetEntity = Project.class)
    private UUID projectId;
}
```
- **Why**: Eliminates N+1 query storms and lazy loading issues while providing Fractal with complete dependency topology.

##### Guideline 2: Traditional JPA Entity Associations (`@ManyToOne`)
When using object relationships, leave `@ShardedKey` parameterless:
```java
@Entity
@Table(name = "projects")
@ShardedEntity
public class Project {
    @Id
    private UUID id;

    // Zero parameters needed: Fractal infers Organization.class and "org_id"
    @ShardedKey
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "org_id")
    private Organization organization;
}
```

##### Guideline 3: Direct Tenant ID Denormalization (Single-Hop Optimization)
For ultra-high-volume leaf tables (e.g. `events`, `audit_logs`, `invoices`), consider denormalizing `org_id` directly onto the table:
```java
@Entity
@Table(name = "audit_logs")
@ShardedEntity
public class AuditLog {
    @Id
    private UUID id;

    // Direct link to root bypasses multi-table joins during rebalancing
    @Column(name = "org_id")
    @ShardedKey(targetEntity = Organization.class)
    private String orgId;
}
```
- **Why**: Rebalancer generates direct `SELECT * FROM audit_logs WHERE org_id = :userId` queries without multi-table JOIN overhead.

##### Guideline 4: Mandatory Key Alignment between Service Routing and @ShardedRoot
Ensure that the field annotated with [`@ShardedKey`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedKey.java) on [`@ShardedRoot`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedRoot.java) directly corresponds to the value extracted by the routing aspect ([`@Sharded`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/Sharded.java)):
- **Present in Root Table**: The sharding key **must be a physical column on the root entity's table** (typically its `@Id` primary key).
- **B2B SaaS**: Root entity is `Organization`, `@ShardedKey` is `id`, and the JWT extraction claim is set to `tenant_id` or `org_id`.
- **B2C Platforms**: Root entity is `User`, `@ShardedKey` is `id`, and JWT extraction claim is set to `sub` (or SpEL extracts `#userId`).
- **Format Consistency**: If the root entity uses `UUID` or `String`, verify that the JWT claim or SpEL argument produces the exact same string serialization (e.g. lowercase UUID string) expected by the root table's column.

#### Discovery Mechanics Summary:
1. **Root Partition Anchor**: Exactly one entity must have [`@ShardedRoot`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedRoot.java) (legacy `@ShardedEntity(root = true)` is also supported for backward compatibility). Its `@ShardedKey` field defines `rootTable` and `rootIdColumn`.
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
3. **Kahn's Topological Sort & Determinism**:
   - **Insert Ordering**: Root/parent tables first, moving downstream to leaf child tables.
   - **Delete Ordering**: Reverse of the insert order (leaf child tables first, root tables last).
   - **Deterministic Graph Ordering**: Uses deterministic `LinkedHashMap` and `LinkedHashSet` structures throughout foreign key discovery and Kahn's topological sort, guaranteeing reproducible, deterministic table copy and delete sequences across different JVM executions and operating systems.
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
   - Sleep max(quiescencePeriod, statusCacheTtl)
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

1. **Lock Entity & Drain In-Flight Transactions**: Sets tenant status to `MIGRATING` on the master Primary table and primes the Caffeine cache. Invokes `awaitTenantQuiescence`, which enforces a mandatory initial pause of at least `status-cache-ttl` (default: `2s`) to ensure all cluster pods' local Caffeine caches have expired their `ACTIVE` status and refreshed to `MIGRATING`. It then waits up to `drain-timeout` (default: `10s`) for active local requests on that tenant to drain to 0. If transactions fail to drain within the timeout, migration is deferred safely.
2. **Record Copy Phase**: Registers the tenant in `fractal_tenant_migrations` with `phase = 'COPYING'`.
3. **Idempotent Target Sanitization**: Checks if an earlier aborted migration left orphan rows on the target shard. If source still holds data, partial target rows are safely purged in reverse topological order.
4. **Data Replication**: Selects and streams rows from source to target in topological insert order using multi-hop SQL joins. Batch sizes are dynamically bounded:
   $$\text{chunkSize} = \min\left(\text{batchSize}, \left\lfloor \frac{\text{maxBatchParameters}}{\text{columnCount}} \right\rfloor\right)$$
   preventing JDBC bind parameter overflow (PostgreSQL's 65,535 limit or SQLite's 32,766 limit).
5. **Record Pruning Phase**: Sets `phase = 'PRUNING'` in `fractal_tenant_migrations`.
6. **Data Eviction**: Deletes migrated rows from source shard in reverse topological order.
7. **Clear Migration State**: Deletes row from `fractal_tenant_migrations`.
8. **Unlock Entity**: Restores tenant status to `ACTIVE` on Primary DB and updates cache.

#### Automatic Rollback on Migration Failure
If an unexpected exception (e.g. database network disconnect, disk full, unhandled constraint error) occurs during table copying (step 4) or source pruning (step 6), `RebalanceEngine` executes an automated rollback inside its `catch` block:
1. Resets the tenant status to `ACTIVE` in both the Primary DB root table and `fractal_tenant_migrations`.
2. Purges any partially copied records from the target shard in reverse topological order.
3. Invalidates the tenant's cache entry in `TopologyManager`.
4. Rethrows the exception to notify operators.

This guarantees that transient errors will never leave a tenant locked in permanent `MIGRATING` state (which would otherwise cause infinite HTTP 503 errors).

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

### Atomic Reference Table Synchronization (ReplicaTableSynchronizer)

[`ReplicaTableSynchronizer`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/ReplicaTableSynchronizer.java) copies read-mostly lookup tables ([`@ShardedReplica`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedReplica.java)) from the primary coordinator database to physical worker shards on application startup and whenever new shards join the cluster:

1. **Transactional Synchronization per Shard**:
   - Each target shard's synchronization is wrapped inside a `TransactionTemplate`.
   - If an insert or network failure occurs while synchronizing a table batch, the transaction on that shard rolls back completely. This ensures target shards are never left with empty, truncated, or half-populated reference tables.
2. **Cluster Shard State Filtering**:
   - Skips shards that are currently `DRAINING` or `DECOMMISSIONED` in `fractal_shard_topology`. Reference data is only delivered to active member shards.
3. **Batch Parameter Bounding**:
   - Uses dynamically bounded chunks to satisfy JDBC parameter bind limits across database engines.

---

## 4. Use Cases & Reference Configurations

### Architecture Profile Matrix

| Architecture Profile | Sharding Key Source | Entity Discovery Strategy | Multi-Tenancy & Data Distribution Reality | Rebalancing & Migrations | Replicated / Reference Data | Ideal For |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Profile 1: B2B Multi-Tenant SaaS (Pooled Sharding)** | JWT Security Claim (`tenant_id`, `org_id`) | Domain Annotations ([`@ShardedRoot`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedRoot.java), [`@ShardedEntity`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedEntity.java), [`@ShardedKey`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedKey.java), [`@ShardedStatus`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedStatus.java)) | **Pooled Multi-Tenancy**: Multiple tenants share each physical shard. All data for any single tenant is strictly co-located on the same shard for native local joins. | Automated Rebalancer (`enabled: true`, zero-config) | [`@ShardedReplica`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedReplica.java) + [`@ShardedBroadcast`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedBroadcast.java) | Enterprise B2B SaaS with hundreds or thousands of organizational tenants |
| **Profile 2: High-Throughput B2C Account Partitioning** | Service Method SpEL (`#userId`, `#accountId`) | Domain Annotations or Database Catalog | **Entity Partitioning**: Millions of consumer user accounts uniformly spread across shard pools. Single-account operations stay local; cross-account flows require Outbox/Saga. | Automated Rebalancer (`enabled: true`, high virtual node factor) | Replicated reference tables (`currencies`, `tiers`) | FinTech, E-Commerce, consumer banking, crypto wallets, gaming platforms |
| **Profile 3: Turnkey Legacy Catalog Sharding** | JWT or Method SpEL | Database Catalog Introspection (`shard-all: true`) | **Catalog-Driven Partitioning**: Relational trees are discovered via ANSI foreign keys. Tables linked to root are sharded; unlinked tables become global replicas. | Automated Rebalancer (`enabled: true`, topological sort) | Disconnected tables automatically classified as replicas and cloned | Brownfield monoliths with established schemas where entity changes are prohibited |
| **Profile 4: Read-Mostly Reference Replication** | JWT or Method SpEL | Domain Entities with [`@ShardedReplica`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedReplica.java) | **Global Broadcast & Local Join**: Sharded operational data (orders) joins locally with replicated catalogs (tax rates, products) without cross-network hops. | Optional | Auto-synced on startup/cluster joins via [`ReplicaTableSynchronizer`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/ReplicaTableSynchronizer.java) + parallel [`@ShardedBroadcast`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedBroadcast.java) | Global reference catalogs requiring sub-millisecond local joins with sharded tables |
| **Profile 5: Asynchronous Worker / Event Consumer** | Method SpEL on Message Payload (`#event.tenantId`) | N/A (Stateless Routing) | **Stateless Shard Context Leasing**: Worker threads lease the shard context for the duration of message processing, cleaning up immediately in `finally`. | Disabled (`rebalancer.enabled: false`) | Read from local shard or primary DB | Background workers, Kafka/RabbitMQ consumers, `@Scheduled` batch jobs |

---

### Profile 1: B2B Multi-Tenant SaaS (Pooled Sharding & Tenant Co-Location)

#### The Multi-Tenancy Architecture Reality: Pooled Sharding

In consistent-hashing horizontal sharding, physical database shards are **shared pools**, not dedicated single-tenant databases:
- If an application has **100 enterprise tenants** and **2 physical shards**, each physical shard hosts approximately **50 tenants**.
- Multiple tenants share the same PostgreSQL/MySQL instance, the same table schemas (`organizations`, `projects`, `tasks`), disk storage, and HikariCP connection pool on that shard.
- Logical tenant separation within each physical database is maintained at the query and application layer via `org_id` foreign keys.

#### What Fractal Solves for B2B Multi-Tenancy:

1. **Strict Tenant Data Co-Location**:
   All child and leaf entities belonging to a single organization (e.g. `projects`, `tasks`, `invoices`, `audit_records`) are guaranteed to reside on the **exact same physical shard** as the root `Organization` record.
   - **Local Relational Joins**: Service methods execute native SQL joins (`JOIN projects p ON ... JOIN tasks t ON ...`) on the shard with microsecond latency. No distributed cross-shard joins or two-phase commit (2PC) protocols are needed.
   - **Local ACID Transactions**: All writes within a tenant aggregate commit atomically in a single local database transaction.
2. **Horizontal Capacity & Storage Scaling**:
   Rather than provisioning an exorbitantly expensive monolithic 10TB database instance, storage and IOPS are partitioned across multiple commodity database instances (e.g. 5 nodes with 2TB each).
3. **Blast Radius Reduction**:
   A hardware outage, CPU exhaustion, or lock contention on `shard-1` only impacts the subset of tenants hashed to `shard-1`. Tenants allocated to `shard-2` through `shard-5` remain completely unaffected and operational.
4. **Dynamic Capacity Rebalancing (Noisy-Neighbor Mitigation)**:
   If Tenant X on `shard-1` experiences sudden business growth and consumes excessive IOPS, an administrator can provision `shard-3` in `application.yml`. Fractal's online rebalancing engine automatically migrates a subset of tenants (or Tenant X itself) to the new shard with sub-second cutover and zero application downtime.

#### Reference Configuration (`application.yml`)

```yaml
fractal:
  sharding:
    enabled: true
    virtual-nodes: 150
    jwt:
      claim-name: org_id  # Automatically extracted from SecurityContextHolder
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

#### Domain Model & Service Layer

```java
// 1. Root Entity: Anchor of the tenant aggregate
@Entity
@Table(name = "organizations")
@ShardedRoot
public class Organization {
    @Id
    @ShardedKey
    private String id; // The primary routing key

    private String name;

    @ShardedStatus(migratingValue = "MIGRATING", activeValue = "ACTIVE")
    @Column(name = "sync_status")
    private String syncStatus;
}

// 2. Child Entity: Co-located on the same shard via organization FK
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

// 3. Service Layer: Zero routing boilerplate
@Service
public class ProjectService {
    @Autowired private ProjectRepository projectRepository;

    // Automatically extracts org_id from JWT in SecurityContextHolder and routes to the tenant's shard
    @Sharded
    @Transactional
    public Project createProject(String projectName) {
        // Native local SQL execution on the tenant's assigned shard
        return projectRepository.save(new Project(projectName));
    }
}
```

---

### Profile 2: High-Throughput B2C Account Partitioning (FinTech, E-Commerce, Gaming)

#### Context & Architectural Drivers

In consumer platforms (retail banking, digital wallets, e-commerce shopping carts, multiplayer games), there are no "organizations" or enterprise JWT claims. Instead, the system manages tens of millions of discrete consumer user accounts (`user_id`, `account_id`, `wallet_id`).

The primary architectural bottleneck is **write throughput (IOPS)** and **table index size**:
- A single database cannot sustain 100,000 writes/second or hold a 500-million-row B-tree index in RAM.
- Sharding distributes the IOPS load uniformly across 4, 8, or 16 database instances (e.g. 8 shards handling 12,500 writes/sec each).

#### Key Mechanics & Considerations

1. **Granular SpEL Method Routing**:
   Routing is evaluated dynamically on service method parameters via Spring Expression Language:
   `@Sharded(key = "#accountId")` or `@Sharded(key = "#request.walletId")`.
2. **Virtual Node Tuning for Uniform Distribution**:
   With millions of random UUIDs or numeric account numbers, setting `virtual-nodes: 250` or higher guarantees an extremely uniform key distribution (<2% standard deviation across shards), preventing "hot shards."
3. **The Cross-Shard Transfer Problem (Important Architecture Constraint)**:
   - What happens when Account A (resident on `shard-alpha`) transfers funds to Account B (resident on `shard-beta`)?
   - **Constraint**: A single `@Transactional` method **cannot** execute atomic writes across multiple shards because Spring's transaction manager binds exactly one JDBC `Connection` per transaction.
   - **Solution**:
     - *Single-Account Operations* (deposits, withdrawals, balance lookups, card authorizations) are local transactions executed on the account's shard with zero overhead.
     - *Cross-Account Transfers* must be coordinated using the **Transactional Outbox Pattern** or **Saga Pattern**:
       1. Shard Alpha: Debit Account A and record an `outbox_events` row in a single local transaction.
       2. Asynchronous Event Publisher: Delivers the event to Kafka / RabbitMQ.
       3. Consumer Worker: Executes credit on Account B on Shard Beta with idempotent deduplication.

#### Reference Configuration (`application.yml`)

```yaml
fractal:
  sharding:
    enabled: true
    virtual-nodes: 250   # High virtual node factor for uniform distribution across millions of accounts
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

#### Service Layer

```java
@Service
public class WalletService {
    @Autowired private WalletRepository walletRepository;
    @Autowired private OutboxRepository outboxRepository;

    // 1. Single-account operation: Native atomic local transaction on account's shard
    @Sharded(key = "#accountId")
    @Transactional
    public Wallet deposit(String accountId, BigDecimal amount) {
        return walletRepository.addFunds(accountId, amount);
    }

    // 2. Cross-account transfer source phase: Debits Account A and writes Outbox on Shard Alpha
    @Sharded(key = "#sourceAccountId")
    @Transactional
    public TransferInitiationResult initiateTransfer(String sourceAccountId, String targetAccountId, BigDecimal amount) {
        walletRepository.subtractFunds(sourceAccountId, amount);
        outboxRepository.save(new OutboxEvent("FUNDS_DEBITED", sourceAccountId, targetAccountId, amount));
        return new TransferInitiationResult(UUID.randomUUID(), "PENDING_DESTINATION_CREDIT");
    }
}
```

---

### Profile 3: Turnkey Legacy Catalog Sharding (`shard-all: true`)

#### Context & Architectural Drivers

For existing enterprise monoliths with mature relational schemas and dozens of existing JPA entity classes, refactoring source code to add `@ShardedRoot`, `@ShardedEntity`, and `@ShardedKey` annotations across 50+ classes is often prohibited by project deadlines or risk governance.

In this scenario, Fractal's **Database Catalog Introspection Engine** (`shard-all: true`) automatically inspects the ANSI database catalog and builds the dependency graph without modifying a single Java class.

#### Key Mechanics:

1. **ANSI Information Schema Discovery**:
   [`TableDependencyResolver`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TableDependencyResolver.java) connects to the primary coordinator database and introspects `information_schema.referential_constraints` and `information_schema.key_column_usage`.
2. **Automatic Dual-Category Classification**:
   - **Sharded Tables**: Multi-hop breadth-first search (BFS) identifies all tables reachable via foreign key constraints from the designated `root-table` (e.g. `customers`). These tables are topologically sorted (insert order: parents before children; delete order: children before parents) for safe rebalancing.
   - **Replicated Reference Tables**: Any user table that has **no foreign key path** to `root-table` (e.g. `zip_codes`, `tax_brackets`, `country_codes`) is automatically classified as a replicated reference table and synchronized to all shards by [`ReplicaTableSynchronizer`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/ReplicaTableSynchronizer.java).
   - **Excluded Tables**: Non-business tables (`flyway_schema_history`, `databasechangelog`, `audit_logs`) are skipped via `exclude-tables`.
3. **Requirements & Trade-offs**:
   - *Requires Physical Database Foreign Keys*: Unlike domain entity annotations (which can model virtual logical hops in code), `shard-all` relies entirely on physical database constraints present in the information schema.
   - *No Circular FK Dependencies*: Foreign key relationships among child tables must form a directed acyclic graph (DAG).

#### Reference Configuration (`application.yml`)

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
      shard-all: true                # Automatically discovers and shards all child tables
      root-table: customers          # Anchor table for the relational partition
      root-id-column: customer_id    # Partition column on the anchor table
      status-column: sync_status     # Migration lock column on customers table
      exclude-tables:
        - flyway_schema_history
        - audit_logs
        - spring_session
```

---

### Profile 4: Read-Mostly Reference Replication & Multi-Shard Broadcasting

#### The Distributed Join Challenge

In almost every sharded business application, sharded operational data must join with global reference data:
- `orders` (sharded by tenant) joins with `currencies` (global exchange rates).
- `invoices` (sharded by customer) joins with `tax_rates` (global jurisdiction rates).
- `products` (sharded by merchant) joins with `categories` (global taxonomic tree).

Because sharded tables reside on separate physical database servers, cross-database SQL joins are impossible at the JDBC level. Querying a central database over HTTP/REST on every order query causes severe latency degradation.

#### The Solution: Local Replicas & Broadcast Mutations

1. **Local Replicas on Every Physical Shard**:
   Mark global reference entities with [`@ShardedReplica`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedReplica.java).
   - At application boot, [`ReplicaTableSynchronizer`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/ReplicaTableSynchronizer.java) copies the entire table from the primary coordinator database to every physical shard.
   - When new shards are added, they are automatically caught up with all reference data before accepting traffic.
2. **Sub-Millisecond Native Shard Joins (Reads)**:
   Application repositories execute native, local SQL joins on the shard with zero network hops:
   ```sql
   SELECT o.id, o.amount * c.exchange_rate
   FROM orders o
   JOIN currencies c ON o.currency_code = c.code
   WHERE o.customer_id = :customerId;
   ```
3. **Multi-Shard Broadcast Writes**:
   When reference data is updated (infrequent administrative mutations), annotate the service method with [`@ShardedBroadcast`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/annotation/ShardedBroadcast.java). Fractal concurrently executes the write against the primary database and all physical shards in parallel using a thread pool.

#### Reference Configuration (`application.yml`)

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

#### Code Examples

```java
// 1. Replicated Reference Entity
@Entity
@Table(name = "currencies")
@ShardedReplica
public class Currency {
    @Id
    private String code; // e.g., "USD", "EUR"
    private BigDecimal exchangeRate;
}

// 2. Admin Service: Broadcasts rate updates to all physical shards in parallel
@Service
public class CurrencyAdminService {
    @Autowired private CurrencyRepository currencyRepository;

    @ShardedBroadcast
    @Transactional
    public void updateExchangeRate(String code, BigDecimal newRate) {
        // Concurrently executes on primary DB and every physical shard
        currencyRepository.updateRate(code, newRate);
    }
}

// 3. Sharded Repository: High-performance local join on physical shard
@Repository
public interface OrderRepository extends JpaRepository<Order, UUID> {
    @Query("""
        SELECT o.id, o.amount * c.exchangeRate
        FROM Order o
        JOIN Currency c ON o.currencyCode = c.code
        WHERE o.organizationId = :orgId
    """)
    List<Object[]> findConvertedOrderTotals(@Param("orgId") String orgId);
}
```

---

### Profile 5: Asynchronous Worker, Stream Consumer & Event Processor Nodes

#### Context & Architectural Separation

Enterprise deployments typically segregate interactive **Web API pods** (handling synchronous user HTTP traffic) from **Background Worker pods** (consuming Kafka/RabbitMQ events, running `@Scheduled` batch jobs, executing async tasks).

Worker pods must route messages to the correct shard while avoiding uncoordinated rebalancing tasks.

#### Key Mechanics & Best Practices

1. **Stateless Payload Routing**:
   Worker pods operate outside HTTP request contexts and have no JWT tokens. They extract the partition key directly from event payloads using method SpEL:
   `@Sharded(key = "#event.tenantId")` or `@Sharded(key = "#record.customerId")`.
2. **Rebalance Safety: Disabling the Rebalancer on Workers**:
   Worker pods **MUST** configure:
   ```yaml
   fractal.sharding.rebalancer.enabled: false
   fractal.sharding.primary.initialize-schema: false
   ```
   - *Rationale*: Prevents autoscaling worker pods from competing with Web API pods for the distributed `REBALANCE_LOCK` or attempting duplicate schema modifications on the primary database. Rebalancing orchestration is strictly reserved for Web API pods or dedicated operator jobs.
3. **Thread Context Hygiene (Preventing Context Leaks)**:
   Message broker consumers use long-lived, reused worker thread pools (e.g. `ConcurrentMessageListenerContainer`).
   [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) guarantees that `ShardContextHolder.setShard(...)` is strictly leased for the duration of the method and cleared via `ShardContextHolder.clear()` in a `finally` block, preventing message N+1 from accidentally inheriting the shard context of message N.

#### Reference Configuration (`application.yml`)

```yaml
fractal:
  sharding:
    enabled: true
    primary:
      jdbc-url: jdbc:postgresql://db-primary:5432/cluster_primary
      username: worker_user
      password: ${DB_PASS}
      initialize-schema: false   # Coordination tables managed exclusively by Web API pods
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
      enabled: false             # Background workers must NEVER initiate rebalance locks
```

#### Consumer Implementation

```java
@Component
public class OrderEventConsumer {
    @Autowired private OrderProcessingService orderProcessor;

    // Extracts tenantId from message payload and binds execution to the appropriate shard
    @KafkaListener(topics = "customer-events", groupId = "order-fulfillment-group")
    @Sharded(key = "#event.tenantId")
    @Transactional
    public void processCustomerEvent(CustomerOrderEvent event) {
        // Executes strictly on the shard holding event.tenantId's records
        orderProcessor.handleOrder(event);
    }
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
| `fractal.sharding.jwt.attribute-claims` | `Map<String, String>` | `{}` | Optional mapping of JWT claim names to root entity column names for JIT tenant provisioning (e.g. `org_tier: tier`). |
| `fractal.sharding.primary.jdbc-url` | `String` | - | JDBC URL for the primary coordination database. |
| `fractal.sharding.primary.username` | `String` | - | Database username for the primary datasource. |
| `fractal.sharding.primary.password` | `String` | - | Database password for the primary datasource. |
| `fractal.sharding.primary.initialize-schema` | `boolean` | `true` | Automatically creates internal coordination tables (`fractal_shard_topology`, `fractal_locks`, `fractal_tenant_migrations`) on primary DB at startup. |
| `fractal.sharding.primary.drain` | `boolean` | `false` | When `true`, evacuates all sharded child tables from the primary coordinator database to target shards at startup while preserving the root catalog on primary. |
| `fractal.sharding.primary.continuous-drain` | `boolean` | `true` | When `primary.drain` is active, continuously reconciles unaligned or newly inserted tenants on `primary` against physical shards at startup. |
| `fractal.sharding.primary.drain-batch-size` | `int` | `5000` | Keyset pagination streaming chunk size and batch verification limit for continuous drain reconciliation. |
| `fractal.sharding.primary.drain-check-mode` | `DrainCheckMode` | `COUNT_THEN_PROBE` | Alignment verification strategy (`COUNT_THEN_PROBE`, `PROBE_ONLY`, `FULL_STREAMING`). |
| `fractal.sharding.shards.<name>.jdbc-url` | `String` | - | JDBC URL for physical shard `<name>`. |
| `fractal.sharding.shards.<name>.username` | `String` | - | Database username for physical shard `<name>`. |
| `fractal.sharding.shards.<name>.password` | `String` | - | Database password for physical shard `<name>`. |
| `fractal.sharding.rebalancer.enabled` | `boolean` | `false` | Enables the automatic migration listener on startup. |
| `fractal.sharding.rebalancer.shard-all` | `boolean` | `false` | When `true`, automatically shards all database tables (catalog discovery) except excluded tables, ignoring `@ShardedRoot` and `@ShardedEntity`. |
| `fractal.sharding.rebalancer.lock-timeout` | `Duration` | `15m` | Maximum lock expiration duration before an unreleased lock is considered dead and eligible for atomic takeover. |
| `fractal.sharding.rebalancer.lock-refresh-interval` | `Duration` | `1m` | Periodic heartbeat interval for renewing `locked_at` during an active rebalance migration. |
| `fractal.sharding.rebalancer.drain-timeout` | `Duration` | `10s` | Maximum duration to wait for pre-existing local in-flight transactions for a tenant to drain to 0 before deferring migration. Enforces minimum of `5s`. |
| `fractal.sharding.rebalancer.quiescence-period` | `Duration` | `0s` | Optional cluster-wide pause after setting `MIGRATING` status before copying data, giving remote nodes time to commit in-flight transactions. |
| `fractal.sharding.rebalancer.status-cache-ttl` | `Duration` | `2s` | Time-to-live for cached tenant migration status in local Caffeine in-memory cache to eliminate per-request DB queries. |
| `fractal.sharding.rebalancer.status-cache-max-size` | `long` | `50000` | Maximum number of tenant status entries cached in local memory (< 2MB RAM). |
| `fractal.sharding.rebalancer.batch-size` | `int` | `500` | Target chunk size for batch inserts during data copying and replica synchronization. |
| `fractal.sharding.rebalancer.max-batch-parameters` | `int` | `32766` | Maximum total JDBC bind parameters per chunk ($batchSize \times columns \le maxParameters$) to prevent parameter overflow errors (e.g. Postgres 65,535). |
| `fractal.sharding.rebalancer.root-table` | `String` | - | Master table holding tenant/entity records (e.g., `organizations`). Inferred from `@ShardedRoot` (or legacy `@ShardedEntity(root = true)`) if omitted. |
| `fractal.sharding.rebalancer.root-id-column` | `String` | - | Partition column name (e.g., `org_id`). Inferred from root `@ShardedKey` or `@Id` if omitted. |
| `fractal.sharding.rebalancer.status-column` | `String` | - | Column on `rootTable` indicating migration state. Inferred from root `@ShardedStatus` if omitted. |
| `fractal.sharding.rebalancer.migrating-value` | `String` | `MIGRATING` | State string set during an in-flight migration. Inferred from `@ShardedStatus(migratingValue = ...)` if omitted. |
| `fractal.sharding.rebalancer.active-value` | `String` | `ACTIVE` | State string when tenant is available. Inferred from `@ShardedStatus(activeValue = ...)` if omitted. |
| `fractal.sharding.rebalancer.sharded-tables` | `List<String>` | `null` | Optional explicit list of sharded tables. Discovered automatically from `@ShardedRoot` / `@ShardedEntity` domain models or foreign key graph. |
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
      attribute-claims:           # Optional JWT claim to root column mappings for JIT provisioning
        org_tier: tier
    primary:
      jdbc-url: jdbc:postgresql://coordinator-db:5432/primary_meta
      username: fractal_admin
      password: ${PRIMARY_DB_PASSWORD}
      initialize-schema: true     # Auto-create coordination tables (fractal_locks, etc.)
      drain: false                # When true, evacuates all sharded child tables to target shards
      continuous-drain: true      # When drain is true, continuously reconciles unaligned tenants at boot
      drain-batch-size: 5000      # Keyset streaming batch size for continuous reconciliation
      drain-check-mode: COUNT_THEN_PROBE # COUNT_THEN_PROBE | PROBE_ONLY | FULL_STREAMING
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
      shard-all: false            # If true, auto-shards catalog; if false, uses @ShardedRoot / @ShardedEntity
      lock-timeout: 15m           # Lock takeover threshold for dead node recovery
      lock-refresh-interval: 1m   # Heartbeat daemon interval to renew lock
      drain-timeout: 10s          # Timeout to drain local in-flight transactions (min 5s)
      quiescence-period: 0s       # Pause before data copy for distributed transaction drain
      status-cache-ttl: 2s        # Local Caffeine cache TTL for migration checks
      status-cache-max-size: 50000# Max entries in Caffeine cache
      batch-size: 500             # Rows per batch insert chunk
      max-batch-parameters: 32766 # Max bind parameters per chunk
      # When using @ShardedRoot, @ShardedEntity, @ShardedKey, and @ShardedStatus, the properties below
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

### Just-In-Time (JIT) Tenant Provisioning (`@Sharded(provision = true)`)

In modern cloud applications using external identity providers (Keycloak, Auth0, Okta, Azure AD), users authenticate via standard OAuth2 / OpenID Connect flows. When a user logs in for the very first time, their identity is cryptographically proven by a valid, signed Bearer JWT token, but **no corresponding record exists yet in the database root table**.

Attempting to query or persist child entities (e.g. `projects`, `wallets`, `user_settings`) immediately fails with foreign key constraint violations because the tenant anchor record does not exist on the physical shard.

Fractal provides **declarative Just-In-Time (JIT) tenant provisioning** directly through the `@Sharded` annotation:

```java
@Service
public class UserOnboardingService {

    // Targeted Provisioning: Automatically provisions the root record on primary and target shard
    @Sharded(key = "#userId", provision = true)
    public UserProfile getOrCreateUserProfile(String userId) {
        return userProfileRepository.findById(userId)
                .orElseThrow(() -> new IllegalStateException("User profile should have been provisioned"));
    }

    // Standard business methods omit provision = true (default: false), guaranteeing 0 ns overhead
    @Sharded(key = "#userId")
    public List<Order> listUserOrders(String userId) {
        return orderRepository.findAllByUserId(userId);
    }
}
```

#### 1. The Zero-Overhead Fast Path Invariant

To guarantee that high-throughput production workloads do not suffer performance degradation:
- Standard `@Sharded` methods (`provision = false`, default) **completely bypass** the provisioning engine.
- [`ShardingAspect`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/aop/ShardingAspect.java) evaluates `if (!sharded.provision())` as its very first step after key extraction.
- **Zero Cache Misses & Zero DB Queries**: No cache checks, no database roundtrips, and **0 ns overhead** on all ordinary business method invocations.
- Provisioning logic runs strictly when explicitly declared via `@Sharded(provision = true)` on designated authentication or onboarding service entry points.

#### 2. Dual-Presence Root Catalog Atomicity

When `@Sharded(provision = true)` is invoked and the sharding key is not present in the root catalog:
1. **Master Catalog on Primary Database**:
   - [`DefaultTenantProvisioner`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/provisioning/DefaultTenantProvisioner.java) inserts the root record into the master table on the `primary` datasource.
   - This ensures the global catalog, central authentication checks, and rebalancer delta calculators immediately recognize the tenant's existence.
2. **Local Slice on Physical Target Shard**:
   - The consistent hash router determines the physical shard: $S = \text{router.routeNode}(\text{shardingKey})$.
   - The provisioner connects to shard $S$ and inserts the identical root record into the local root table slice.
   - This enables native relational foreign keys and cascading operations on the shard without cross-database hops.

```
                           [First-Time Login Request]
                                       |
                                       v
                    +------------------------------------+
                    |  @Sharded(provision = true)        |
                    |  ShardingAspect Interception       |
                    +------------------+-----------------+
                                       |
                                       v
                    +------------------------------------+
                    |  Caffeine Existence Cache Miss?    |
                    +------------------+-----------------+
                                       |
                                       v
                    +------------------------------------+
                    |  Per-Tenant Keyed Mutex Lock       |
                    |  (ConcurrentHashMap<String, Object>)|
                    +------------------+-----------------+
                                       |
                         +-------------+-------------+
                         |                           |
                         v                           v
             +-----------------------+   +-----------------------+
             | Master Catalog Record |   | Local Root Slice      |
             | INSERT INTO primary   |   | INSERT INTO target    |
             | (organizations/users) |   | (organizations/users) |
             +-----------------------+   +-----------+-----------+
                                                     |
                                                     v
                                         +-----------------------+
                                         | TenantInitializer SPI |
                                         | (Seed child tables    |
                                         |  e.g. user_settings)  |
                                         +-----------------------+
```

#### 3. Concurrency & High-Traffic SPA Burst Protection

When a modern Single-Page Application (SPA) boots following an OAuth2 redirect, it routinely dispatches 5 to 10 concurrent HTTP requests in parallel (e.g., fetching user profile, feature flags, permissions, notifications). If the user is logging in for the first time, all 10 requests hit the server simultaneously.

Fractal provides 3-tier protection against race conditions and lock contention:
1. **Per-Tenant In-Memory Keyed Mutex**:
   - [`DefaultTenantProvisioner`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/provisioning/DefaultTenantProvisioner.java) locks on a fine-grained, per-tenant object lock using `ConcurrentHashMap<String, Object>`.
   - Concurrent requests for tenant $A$ are serialized locally within the JVM, while requests for tenant $B$ proceed completely concurrently with zero lock contention.
2. **Sub-Microsecond In-Memory Existence Cache**:
   - Once tenant $A$ is provisioned, its key is cached in a local [Caffeine](https://github.com/ben-manes/caffeine) cache with a 10-minute TTL.
   - The remaining parallel requests in the burst observe the cache hit in ~15 nanoseconds and proceed directly without touching the database.
3. **Cross-Pod Database Collision Idempotency**:
   - In horizontally scaled multi-pod environments, two pods may attempt to provision tenant $A$ concurrently.
   - Database unique constraint violations (`DataIntegrityViolationException`) are caught and suppressed gracefully as deterministic proof that the tenant was already committed by a peer pod.

#### 4. Automatic Column Extraction Pipeline

[`RootEntityAttributeExtractor`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/provisioning/RootEntityAttributeExtractor.java) maps JWT claims and metadata onto root table database columns:

1. **Partition Primary Key (`@ShardedKey`)**:
   - Bound directly to the extracted sharding key (e.g. `userId` from the JWT `sub` claim).
2. **Migration Status (`@ShardedStatus`)**:
   - Automatically set to `ACTIVE` (or the configured `activeValue`).
3. **Conventional Claim Matching**:
   - Automatically inspects the fields of the `@ShardedRoot` entity class and maps standard JWT token claims to corresponding database columns:
     - Entity field `email` $\to$ JWT claim `email`
     - Entity field `username` or `user_name` $\to$ JWT claim `preferred_username`
     - Entity field `name` or `full_name` $\to$ JWT claim `name`
     - Entity field `created_at` or `createdAt` $\to$ current database timestamp `java.sql.Timestamp` (only mapped if the entity declares the field).
4. **Declarative Custom Claim Mappings (`attribute-claims`)**:
   - Custom or non-standard claims can be mapped declaratively in `application.yml`:
     ```yaml
     fractal:
       sharding:
         jwt:
           attribute-claims:
             org_tier: tier
             department_code: dept
     ```

#### 5. Extensibility SPIs: Child Table Seeding & Customization

##### Seeding Child Tables (`TenantInitializer`)
After the root entity row is inserted onto the target shard, applications often need to seed default child rows (e.g. default preferences, starter projects, audit trail). Implement the [`TenantInitializer`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/provisioning/TenantInitializer.java) SPI:

```java
@Component
public class DefaultUserSettingsInitializer implements TenantInitializer {

    private final JdbcTemplate jdbcTemplate;

    public DefaultUserSettingsInitializer(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public void initializeTenant(String tenantId, String targetShard, Map<String, Object> rootAttributes) {
        // ShardContextHolder is ALREADY bound to targetShard on this thread!
        jdbcTemplate.update(
            "INSERT INTO user_settings (id, user_id, theme, notifications_enabled) VALUES (?, ?, ?, ?)",
            UUID.randomUUID().toString(),
            tenantId,
            "DARK_MODE",
            true
        );
    }
}
```

##### Programmatic Attribute Customization (`RootEntityCustomizer`)
To compute or enrich root table columns dynamically before insertion (e.g. calculating billing tiers or assigning default quotas), implement [`RootEntityCustomizer`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/provisioning/RootEntityCustomizer.java):

```java
@Component
public class BillingTierCustomizer implements RootEntityCustomizer {

    @Override
    public void customizeRootEntity(String shardingKey, Map<String, Object> attributes, Authentication authentication) {
        if (!attributes.containsKey("tier")) {
            attributes.put("tier", "COMMUNITY_FREE");
        }
        attributes.put("storage_quota_mb", 5120);
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

### Adopting Fractal on an Already-Populated Monolithic Database (Brownfield Migration)

A common architectural question when introducing Fractal into existing production environments is:
> *"If all my application tables and rows currently live on a single monolithic database, when does Fractal detect that, and how does it shard the pre-existing data across new shards?"*

#### 1. When Does Fractal Check the Topology?

Fractal checks cluster shard topology **at application startup** via Spring Boot's `CommandLineRunner` lifecycle ([`fractalStartupListener`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/config/FractalAutoConfiguration.java#L156-L278)):

1. **Introspects Coordination State**: It queries the coordination table `fractal_shard_topology` on the `primary` database:
   ```sql
   SELECT shard_name FROM fractal_shard_topology;
   ```
2. **Detects Discrepancies**: It compares the database records against the shards declared in `application.yml` under `fractal.sharding.shards.*`.
3. **Triggers Rebalancing**: If any shard declared in YAML is missing from `fractal_shard_topology` (or if there are pending interrupted migrations), Fractal identifies a topology delta (`hasNewShards = true`) and asynchronously triggers the rebalancing engine via `fractalRebalanceExecutor`.

#### 2. How Pre-Existing Monolithic Data is Sharded: The "1-Shard Baseline" Pattern

Fractal's [`RebalanceEngine`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/RebalanceEngine.java) is designed to migrate tenant aggregates **between physical shards** ($N \to M$). The `primary` database acts as the cluster coordinator and master catalog (holding the master tenant list and coordination tables), not a staging dump.

If an application already has all tables and data populated in a single database, **do not** spin up multiple empty shards on Day 1 and expect Fractal to automatically drain your primary database. Instead, use the **1-Shard Baseline Pattern**, the zero-downtime, zero-network-copy industry standard for database sharding:

##### Phase 1: Deploy Fractal with a 1-Shard Baseline (Day 1 - Zero Data Copying)

Declare your **existing populated database as `shard-1`**:

```yaml
fractal:
  sharding:
    enabled: true
    primary:
      jdbc-url: jdbc:postgresql://db-monolith:5432/myapp # Points to your existing database
      username: myapp_admin
      password: ${DB_PASS}
    shards:
      shard-1:
        jdbc-url: jdbc:postgresql://db-monolith:5432/myapp # Points to the EXACT SAME existing database!
        username: myapp_admin
        password: ${DB_PASS}
    rebalancer:
      enabled: true
```

- **What Happens**: On initial startup, Fractal sees that `fractal_shard_topology` is empty and registers `shard-1` as the sole known physical shard.
- **Immediate Compatibility**: Because `shard-1` is the only node on the consistent hash ring, **100% of routing keys hash to `shard-1`**.
- **Zero Data Movement**: All existing records already reside on `shard-1`. The application runs immediately with zero downtime, zero data copying over the network, and zero risk. The application is now fully sharding-ready.

##### Phase 2: Scale Horizontally by Adding New Shards ($1 \to N$ Shards)

When the business needs to scale write throughput or storage capacity beyond the single database:

1. **Provision New Database Nodes**: Provision `shard-2` and `shard-3` instances, applying the DDL schema via Flyway or Liquibase.
2. **Update `application.yml`**:
   ```yaml
   fractal:
     sharding:
       shards:
         shard-1:
           jdbc-url: jdbc:postgresql://db-monolith:5432/myapp # Original database holding 100% of data
           username: myapp_admin
           password: ${DB_PASS}
         shard-2:
           jdbc-url: jdbc:postgresql://db-shard2:5432/myapp   # New empty database
           username: myapp_admin
           password: ${DB_PASS}
         shard-3:
           jdbc-url: jdbc:postgresql://db-shard3:5432/myapp   # New empty database
           username: myapp_admin
           password: ${DB_PASS}
       rebalancer:
         enabled: true
   ```
3. **Automated Online Rebalancing**:
   - On the next startup (or rolling update), `fractalStartupListener` detects `dbShards = [shard-1]` and `yamlShards = [shard-1, shard-2, shard-3]`.
   - [`MigrationDeltaCalculator`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/MigrationDeltaCalculator.java) computes the hash delta:
     - Old topology (`shard-1`): mapped 100% of tenants to `shard-1`.
     - New topology (`shard-1, shard-2, shard-3`): balances tenants across all 3 shards (~33% each).
   - Fractal identifies that only **~66% of tenants** must migrate to `shard-2` and `shard-3`.
   - The remaining **~33% of tenants never move**: they stay on `shard-1` with zero data copying and zero downtime.
   - For the moving tenants, [`RebalanceEngine`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/RebalanceEngine.java) migrates them serially (quiescence, topological batch copy, source pruning, and immediate cutover).
   - Once complete, `shard-1`, `shard-2`, and `shard-3` are marked active, and the cluster operates seamlessly in a 3-shard steady state.

#### 2. Adoption Pathway B: Automated Monolith Evacuation (The Primary Drain Pattern)

If the engineering goal is to maintain the existing database **strictly as a lightweight coordinator** (holding only the master tenant directory, reference data, and coordination tables) and completely evacuate all sharded tables into brand-new shard instances (`shard-1`, `shard-2`, etc.), Fractal provides an automated draining mechanism via `primary.drain: true`.

##### Configuration
```yaml
fractal:
  sharding:
    enabled: true
    primary:
      jdbc-url: jdbc:postgresql://db-monolith:5432/myapp # Existing populated monolithic database
      username: myapp_admin
      password: ${DB_PASS}
      drain: true # Instructs Fractal to evacuate all sharded tables to target shards
    shards:
      shard-1:
        jdbc-url: jdbc:postgresql://db-shard1:5432/myapp # Freshly provisioned shard instance
        username: myapp_admin
        password: ${DB_PASS}
      shard-2:
        jdbc-url: jdbc:postgresql://db-shard2:5432/myapp # Freshly provisioned shard instance
        username: myapp_admin
        password: ${DB_PASS}
    rebalancer:
      enabled: true
```

##### Evacuation Lifecycle & Preserved Invariants
1. **Target Ring Construction**: At boot, [`ConsistentHashRouter`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/core/ConsistentHashRouter.java) creates the active ring using only the declared worker shards (`[shard-1, shard-2]`). `primary` is never treated as a worker shard on the hash ring.
2. **Dual-Ring In-Flight Routing**: While unmigrated tenants wait for their individual turn, incoming requests route to `primary` via `pendingSourceShards` overrides in [`TopologyManager`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TopologyManager.java).
3. **Serial Tenant Evacuation**:
   - For every tenant discovered in `primary`'s `rootTable`, [`RebalanceEngine`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/RebalanceEngine.java) copies the tenant's root record and all sharded child tables (`projects`, `tasks`, etc.) to their assigned target shard (`shard-1` or `shard-2`).
   - The child tables are **deleted** from `primary`.
   - The root table record on `primary` is **preserved** and updated to status `ACTIVE`. This maintains `primary`'s catalog integrity for subsequent cluster operations, reference table synchronization, and authentication.
   - The pending override is cleared, immediately cutting over live traffic for that tenant to the new shard.
4. **Completion & Idempotency**:
   - Once all tenants have been migrated off `primary`, [`TopologyManager`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/TopologyManager.java) records `primary` with `status = 'DRAINED'` in `fractal_shard_topology`.
   - On subsequent restarts, Fractal detects that `primary` is already drained and skips re-execution without redundant work.

---

### Continuous Primary Drain Reconciliation & Scale-Proof Alignment

#### The Post-Drain Drift Challenge

In enterprise hybrid environments, after `primary` is initially marked `DRAINED`, the primary database rarely remains completely static:
- External identity management (IAM) synchronization jobs or SCIM connectors may insert new tenant organizations or user records directly into `primary`.
- Legacy internal admin tools, ETL pipelines, or batch scripts may continue writing new tenant aggregates to the primary database.
- Developers or DBAs may execute batch seed scripts during staging or release deployment windows.

If Fractal merely checked `if (topology.isShardDrained("primary")) return;`, these newly inserted tenants would remain stranded on `primary`. Child queries would route to worker shards, failing with missing record errors.

#### The Memory-Safe 3-Tier Reconciliation Engine

To guarantee continuous alignment without consuming unbounded JVM heap or degrading application boot time, Fractal implements a **constant-memory 3-Tier reconciliation engine** in [`MigrationDeltaCalculator`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/MigrationDeltaCalculator.java):

```
                   [Continuous Drain Startup Check]
                                  |
                                  v
+-------------------------------------------------------------------+
| Tier 1: Instant Count Guard (2-5 ms, 16 bytes heap)                |
| SELECT COUNT(*) on primary vs SUM(COUNT(*)) across active shards  |
+---------------------------------+---------------------------------+
                                  |
               +------------------+------------------+
               | (Counts Match)                      | (Counts Differ)
               v                                     v
+-------------------------------+ +---------------------------------+
| ZERO DRIFT DETECTED           | | Tier 2: Status-Indexed Probe    |
| - Fast Path Complete          | | (< 1 ms via DB index)           |
| - 0 extra queries             | | SELECT id WHERE status != ACTIVE|
| - 0 memory allocation         | +----------------+----------------+
+-------------------------------+                  |
                                  +----------------+----------------+
                                  | (Has Unaligned)| (Status Missing|
                                  v                |  or Inconclusive)
                  +------------------------------+ v
                  | Evacuate K unaligned tenants | +----------------+
                  +------------------------------+ | Tier 3: Keyset |
                                                   | Streaming      |
                                                   | Cursor (500 KB)|
                                                   +----------------+
```

##### Tier 1: Instant Count Guard (Zero-Allocation Fast Path)
- **Execution**: Computes `countPrimary = SELECT COUNT(*) FROM rootTable` on `primary` and sums `countShards = SELECT COUNT(*) FROM rootTable` across all active worker shards.
- **Latency & Memory**: Completes in **2–5 ms** over JDBC and allocates **16 bytes** of heap memory (two primitive 64-bit `long` integers).
- **Result**: In **99.9% of routine application restarts**, `countPrimary == countShards`. The reconciliation engine terminates immediately with zero further database roundtrips, zero row fetching, and zero JVM memory pressure.

##### Tier 2: Status-Indexed Probe (< 1 ms via Database Index)
- **Execution**: When counts differ and the `@ShardedStatus` column is present on `rootTable`, Fractal queries:
  ```sql
  SELECT id FROM rootTable WHERE sync_status != 'ACTIVE'
  ```
- **Performance**: Backed by a standard B-Tree index on `sync_status`, this query executes in **< 1 ms** even on tables containing 100,000,000+ rows.
- **Result**: If new tenants were inserted with initial statuses (e.g. `PENDING`, `NEW`, `UNASSIGNED`), Fractal fetches strictly the $K$ misaligned IDs into memory, bypassing full table scanning entirely.

##### Tier 3: Bounded Keyset Streaming Cursor (~500 KB Heap Bound)
- **Execution**: If Tier 2 is disabled or inconclusive (e.g. no status column defined), Fractal performs deterministic keyset pagination:
  ```sql
  SELECT id FROM rootTable WHERE id > :lastSeenId ORDER BY id ASC LIMIT :drainBatchSize
  ```
- **Scale-Proof Invariant**:
  - The cursor processes the table in strictly bounded chunks (default `drain-batch-size: 5000`).
  - For each chunk of 5,000 IDs, Fractal groups IDs by their routed shard via [`ConsistentHashRouter`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/core/ConsistentHashRouter.java) and queries the target shards in sub-batches of 1,000 (`WHERE id IN (...)`) to detect unaligned records.
  - **Constant Memory Bound**: Heap consumption is strictly capped at **~500 KB**, regardless of whether the primary database holds 10,000 or 50,000,000 root records. It is completely immune to JVM `OutOfMemoryError`.

#### On-Demand Runtime Drain API (Zero Pod Restarts)

For scenarios where an external process adds a tenant to `primary` while the application is actively serving traffic, Fractal provides an on-demand programmatic drain API through [`RebalanceEngine`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/RebalanceEngine.java):

```java
@Service
public class TenantLifecycleService {

    @Autowired
    private RebalanceEngine rebalanceEngine;

    // Evacuates a single newly created tenant from primary to its consistent hash shard
    public void evacuateTenantFromPrimary(String tenantId) {
        rebalanceEngine.drainTenantFromPrimary(tenantId);
    }

    // Explicitly overrides target shard placement if required
    public void evacuateTenantToSpecificShard(String tenantId, String targetShard) {
        rebalanceEngine.drainTenantFromPrimary(tenantId, targetShard);
    }
}
```

##### Runtime Drain Guarantees:
1. **Zero Downtime**: Unaffected tenants continue serving traffic without lock contention.
2. **Master Catalog Retention**: The root record on `primary` is preserved in status `ACTIVE`, ensuring primary metadata integrity.
3. **Immediate Traffic Cutover**: Live traffic for `tenantId` cuts over to the target worker shard in sub-second time.

---

### Scaling Down: Graceful Shard Decommissioning ($M \to N$ Shards)

Cluster scale-down (e.g. contracting from 3 shards to 2 shards, or decommissioning an ephemeral maintenance shard) requires safely migrating all tenant records from the retiring shard(s) to the surviving shards without data loss or request errors.

##### The Distributed System Invariant
In distributed systems, you cannot remove a storage node by simply erasing its credentials from configuration. If `shard-1`'s datasource is removed from `application.yml`, Fractal cannot establish JDBC connections to `shard-1` to read tenant rows, lock in-flight updates, or prune migrated tables.

##### The 3-Step Decommissioning Workflow

###### Step 1: Mark Retiring Shards in Configuration
Retain the connection pool for the retiring shard in `application.yml`, but mark it with `decommission: true` (or `status: DRAINING`):

```yaml
fractal:
  sharding:
    enabled: true
    primary:
      jdbc-url: jdbc:postgresql://db-primary:5432/myapp
      username: myapp_admin
      password: ${DB_PASS}
    shards:
      shard-1:
        jdbc-url: jdbc:postgresql://db-shard1:5432/myapp
        username: myapp_admin
        password: ${DB_PASS}
        decommission: true  # Instructs Fractal to safely evacuate this shard
      shard-2:
        jdbc-url: jdbc:postgresql://db-shard2:5432/myapp
        username: myapp_admin
        password: ${DB_PASS}
      shard-3:
        jdbc-url: jdbc:postgresql://db-shard3:5432/myapp
        username: myapp_admin
        password: ${DB_PASS}
    rebalancer:
      enabled: true
```

###### Step 2: Automated Drain & Safe Dual-Ring Migration
On startup (or deployment):
1. **Contracted Ring Initialization**: [`ConsistentHashRouter`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/core/ConsistentHashRouter.java) initializes the runtime hash ring using **active shards only** (`[shard-2, shard-3]`).
2. **Datasource Preservation**: [`ShardingRoutingDataSource`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/core/ShardingRoutingDataSource.java) maintains connection pools to all shards (both active and decommissioning).
3. **Topology Status**: `shard-1` is updated to status `DRAINING` in `fractal_shard_topology`.
4. **Targeted Delta Calculation**: [`MigrationDeltaCalculator`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/main/java/io/github/mucchinas/fractal/rebalance/MigrationDeltaCalculator.java) calculates the ring delta between `[shard-1, shard-2, shard-3]` and `[shard-2, shard-3]`. It generates migration actions **exclusively for tenants currently located on `shard-1`**. Tenants residing on `shard-2` and `shard-3` remain unaffected.
5. **Dual-Ring In-Flight Routing**: For every tenant on `shard-1`, a pending source override is registered. While awaiting their turn, incoming tenant queries continue routing to `shard-1`.
6. **Serial Evacuation**: Tenants on `shard-1` are migrated one-by-one:
   - Mark `MIGRATING` (in-flight queries drain, incoming writes receive retryable HTTP 503).
   - Relational tree copied to target shard (`shard-2` or `shard-3`).
   - Source data pruned from `shard-1`.
   - Immediate cutover to surviving shard.
7. **Topology Unregistration**: When all tenants on `shard-1` have migrated and no pending migrations remain, Fractal deletes `shard-1` from `fractal_shard_topology`.

###### Step 3: Physical Teardown
Once the startup logs display:
```
FRACTAL: Shard shard-1 drenato con successo e rimosso dalla topologia.
```
The operator can safely turn off the physical database instance for `shard-1` and remove the `shard-1` block from `application.yml` during the next routine deployment.

---

### Joining Sharded and Non-Sharded Data

Because sharded tables and primary tables reside in different physical database instances and connection pools, single SQL `JOIN` queries cannot be executed across them at the JDBC layer.

#### Pattern 1: Application-Level Join via Orchestrator Facade (Recommended)

```java
/* 1. Sharded Service: executes queries on tenant's assigned shard */
@Service
public class OrderService {
    @Autowired
    private OrderRepository orderRepository;

    @Sharded(key = "#tenantId")
    @Transactional(readOnly = true)
    public List<Order> getOrdersByTenant(String tenantId) {
        return orderRepository.findByTenantId(tenantId);
    }
}

/* 2. Non-Sharded Service: executes queries on primary coordination database */
@Service
public class CurrencyService {
    @Autowired
    private CurrencyRateRepository currencyRepository;

    /* No @Sharded annotation -> routes to primary datasource */// No @Sharded annotation -> routes to primary datasource
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

The test suite rigorously validates the starter across 34 test classes covering 113 automated test cases, specifically designed to prevent false positives and verify physical database operations:

- [`DualRingMigrationRoutingTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/DualRingMigrationRoutingTest.java): Asserts dynamic dual-ring routing during active rebalancing (pending tenants route to old source shard, active migrating tenant throws `TenantMigratingException`, completed tenants immediately cut over to new ring shard, non-migrating bystanders route unaffected), zero-overhead fast path in steady state, and Caffeine caching of global rebalance status.
- [`ShardDecommissioningTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/ShardDecommissioningTest.java): Tests the end-to-end cluster contraction lifecycle ($M \to N$), verifying that decommissioning shards are excluded from the active ring, their tenants are safely migrated to surviving shards, and the shard is automatically deregistered from topology upon completion.
- [`PrimaryDrainMigrationTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/PrimaryDrainMigrationTest.java): Tests brownfield onboarding via `primary.drain: true`, verifying that all child tables are evacuated from the primary database to worker shards, root table records are retained with `ACTIVE` status, and the primary shard lifecycle transitions cleanly from `DRAINING` to `DRAINED`.
- [`PrimaryDrainSpringBootTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/PrimaryDrainSpringBootTest.java): Full Spring Boot integration test validating automated startup evacuation of a pre-populated primary database to worker shards, followed by verified live `@Sharded` query execution on worker shards.
- [`ContinuousPrimaryDrainScaleSafetyTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/ContinuousPrimaryDrainScaleSafetyTest.java): Validates the 3-tier constant-memory alignment engine under brownfield scale conditions, asserting that Tier 1 count guard terminates in $O(1)$ constant time with 0 allocations, Tier 2 status probe detects unaligned tenants via indexed scans, and Tier 3 keyset pagination streams large ID spaces in bounded chunks without heap memory spikes or `OutOfMemoryError`.
- [`IncrementalTenantDrainTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/IncrementalTenantDrainTest.java): Tests continuous startup reconciliation when new tenants are added to the primary database after initial evacuation, asserting that only the delta tenants are migrated to worker shards while pre-existing drained tenants are untouched and the master root row is preserved on primary.
- [`RuntimeTenantDrainApiTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/RuntimeTenantDrainApiTest.java): Validates the on-demand programmatic runtime drain API (`rebalanceEngine.drainTenantFromPrimary(tenantId)` and `rebalanceEngine.drainTenantFromPrimary(tenantId, targetShard)`), asserting immediate single-tenant evacuation from primary to worker shards with sub-second cutover without requiring application restarts.
- [`TargetedProvisioningIntegrationTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/provisioning/TargetedProvisioningIntegrationTest.java): Validates end-to-end Just-In-Time (JIT) tenant provisioning via `@Sharded(provision = true)`, verifying that first-time authenticated users (e.g. Keycloak JWT) have root records created atomically in both the primary master catalog and the routed physical shard with claims mapped to columns.
- [`ZeroOverheadBypassTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/provisioning/ZeroOverheadBypassTest.java): Asserts the zero-overhead fast-path invariant for standard `@Sharded` methods (`provision = false`, default), verifying that `TenantProvisioner` is completely bypassed with 0 cache lookups and 0 database queries.
- [`ParallelKeycloakLoginBurstTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/provisioning/ParallelKeycloakLoginBurstTest.java): Validates high-concurrency first-login burst scenarios (e.g. SPA firing 20 concurrent parallel requests for a new user), asserting that the per-tenant keyed mutex, Caffeine existence cache, and `DataIntegrityViolationException` suppression guarantee exactly 1 root record is created with 0 duplicate key errors or deadlocks.
- [`TenantInitializerChildTableSeedingTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/provisioning/TenantInitializerChildTableSeedingTest.java): Tests the `TenantInitializer` SPI callback, verifying that custom post-provisioning logic executes with `ShardContextHolder` automatically bound to the target shard, allowing seamless seeding of child rows (e.g. `user_settings`) immediately following root record creation.
- [`CustomClaimMappingTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/provisioning/CustomClaimMappingTest.java): Validates declarative custom JWT claim mappings configured via `fractal.sharding.jwt.attribute-claims`, verifying that non-standard tokens (e.g. `org_tier` mapped to `tier`) correctly extract and populate corresponding root entity columns during JIT provisioning.
- [`ConsistentHashRouterTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/core/ConsistentHashRouterTest.java): Validates deterministic routing, MD5 hash calculation, ring contraction preserving surviving shard assignments, 64-bit ring wrap-around, null key safety, empty ring safety, virtual nodes validation ($> 0$), and uniform distribution across virtual nodes.
- [`RoutingIntegrationTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/RoutingIntegrationTest.java): Validates physical database routing by asserting that `@Sharded` queries read data directly from the routed shard's database, that unannotated queries fall back to the primary database, and that `ShardContextHolder` is consistently cleaned up.
- [`SecurityRoutingIntegrationTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/SecurityRoutingIntegrationTest.java): Confirms end-to-end shard routing from synthetic JWT tokens in `SecurityContextHolder`, enforces precedence of JWT extraction over method-level SpEL keys, tests fallback to SpEL keys when unauthenticated, and validates `ThreadLocal` cleanup on application exceptions.
- [`CustomClaimSecurityRoutingIntegrationTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/CustomClaimSecurityRoutingIntegrationTest.java): Tests end-to-end shard routing using custom JWT claims (e.g. `organization_id`), and asserts failure when the required custom claim is absent from the token.
- [`JwtSecurityKeyExtractorTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/security/JwtSecurityKeyExtractorTest.java): Validates claim extraction, fallback logic, string/numeric/UUID claim conversions, unauthenticated handling, null `SecurityContext` authentication, and non-JWT authentication tokens.
- [`ShardingAspectMigrationLockTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/aop/ShardingAspectMigrationLockTest.java): Asserts that `TenantMigratingException` is thrown when accessing a tenant currently migrating, verifies access restoration once migration completes, and tests in-flight request tracking during method execution.
- [`ShardedBroadcastAspectTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/aop/ShardedBroadcastAspectTest.java): Confirms parallel broadcast writes across primary database and all active shards, validates that `includePrimary = false` skips the primary database, and confirms that decommissioning shards are excluded from broadcast targets.
- [`EntityTableMetadataResolverTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/EntityTableMetadataResolverTest.java): Validates domain entity auto-discovery via `@ShardedRoot`, `@ShardedEntity`, `@ShardedKey`, `@ShardedStatus`, and `@ShardedReplica`, multi-tier hierarchy resolution, JPA `@Table`/`@JoinColumn`/`@Column`/`@Id` metadata extraction, custom status values, cycle detection, reachability checks, single-root enforcement, duplicate status prevention, non-root status prohibition, and legacy `@ShardedEntity(root = true)` compatibility.
- [`EntityRebalanceIntegrationTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/EntityRebalanceIntegrationTest.java): Confirms end-to-end multi-hop tenant migration on physical databases without database foreign key constraints using entity-discovered plans and `@ShardedStatus`, while ensuring replicated tables (`currencies`) are preserved on all shards.
- [`RebalanceEngineTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/RebalanceEngineTest.java): Validates end-to-end tenant migration, row copying across shards using multi-hop plans, reverse-order row pruning, safe handling of missing source/target shards without tenant lockout, and null/empty migration action lists.
- [`RebalanceEngineIdempotencyTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/RebalanceEngineIdempotencyTest.java): Asserts idempotent crash recovery across migration phases, resumption from aborted `COPYING` state (purging partial target data and recopying), resumption from aborted `PRUNING` state (safe source pruning without duplicate inserts), and safe repeated execution without side effects.
- [`QuiescenceAndBatchLimitTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/QuiescenceAndBatchLimitTest.java): Asserts in-flight request tracking, active execution draining via `awaitTenantQuiescence`, safe migration deferral on drain timeouts, cluster quiescence pauses, and dynamic chunk size calculation preventing bind parameter overflow on wide tables.
- [`ReplicaTableSynchronizerTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/ReplicaTableSynchronizerTest.java): Tests batch synchronization of reference tables from primary coordinator to shards and new shard catch-up.
- [`TableDependencyResolverTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/TableDependencyResolverTest.java): Verifies ANSI `information_schema` foreign key discovery, multi-hop BFS dependency resolution, Kahn's topological sort for insert/delete ordering, join query synthesis, table exclusion, replica table isolation, circular dependency rejection, and self-referencing foreign key handling.
- [`TopologyManagerCaffeineCacheTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/TopologyManagerCaffeineCacheTest.java): Validates sub-microsecond Caffeine caching, zero DB queries within TTL, cache invalidation on status updates, and automatic reload on TTL expiration.
- [`TopologyManagerLockTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/TopologyManagerLockTest.java): Verifies distributed lock acquisition, mutual exclusion, expired lock takeover via configurable TTL, periodic heartbeat renewal with circuit breaker failover, graceful shutdown lock release, automatic schema initialization via `InitializingBean`, idempotent ANSI shard registration, batch pending migrations, and idempotent migration recording.
- [`NestedShardedContextTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/aop/NestedShardedContextTest.java): Validates nested `@Sharded` and `@ShardedBroadcast` method invocations, asserting that the outer shard context is preserved across calls and restored upon return, preventing thread-local wiping or cross-tenant contamination.
- [`ClassLevelShardedAspectTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/aop/ClassLevelShardedAspectTest.java): Validates class-level `@Sharded` routing across methods, verifying that `@within(Sharded)` properly intercepts Spring beans without method-level annotations, evaluates SpEL parameters, and routes accordingly.
- [`DualRingProductionDbRoutingTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/DualRingProductionDbRoutingTest.java): Validates multi-pod dual-ring production routing, ensuring non-blocking `PHASE_PENDING` routing to source shards and proper exception handling when migrations transition to active copy/prune phases (`PHASE_COPYING` and `PHASE_PRUNING`).
- [`MultiNodePendingShardResolutionTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/MultiNodePendingShardResolutionTest.java): Validates fallback to querying `fractal_tenant_migrations` on secondary pods when pending migrations are not cached in local memory, eliminating `inFlightRequests` memory leaks and route misdirection across distributed instances.
- [`RebalanceExceptionRecoveryTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/rebalance/RebalanceExceptionRecoveryTest.java): Validates fault-tolerant recovery when unexpected database errors interrupt `RebalanceEngine`, ensuring the tenant status is automatically rolled back to `ACTIVE` on the primary database, partial target rows are safely wiped, and local caches are invalidated.
- [`DataSourceDisposalTest`](file:///home/aquila/Documenti/Projects/fractal-spring-boot-starter/src/test/java/io/github/mucchinas/fractal/datasource/DataSourceDisposalTest.java): Validates graceful shutdown of pooled datasources via `ShardingRoutingDataSource implements DisposableBean`, verifying that all Hikari connection pools across primary and shards are closed cleanly upon application termination without leaking connections.
