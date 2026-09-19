-- ====================================================================
-- Fractal Spring Boot Starter - Primary Coordinator Schema
-- ====================================================================
-- This schema contains the metadata and coordination tables required by Fractal.
-- They reside on the primary datasource and are automatically created at startup
-- if fractal.sharding.primary.initialize-schema is set to true (default).
--
-- For environments using Flyway or Liquibase with strict DDL policies,
-- set fractal.sharding.primary.initialize-schema=false and include this script.
-- ====================================================================

-- 1. Shard Topology Table
CREATE TABLE IF NOT EXISTS fractal_shard_topology (
    shard_name VARCHAR(255) PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Distributed Locks Table with Timestamp
CREATE TABLE IF NOT EXISTS fractal_locks (
    lock_name VARCHAR(255) PRIMARY KEY,
    locked_by VARCHAR(255) NOT NULL,
    locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. In-flight Tenant Migrations Table (for crash recovery and idempotent retries)
CREATE TABLE IF NOT EXISTS fractal_tenant_migrations (
    tenant_id VARCHAR(255) PRIMARY KEY,
    source_shard VARCHAR(255) NOT NULL,
    target_shard VARCHAR(255) NOT NULL,
    phase VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
