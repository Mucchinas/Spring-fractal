package io.github.mucchinas.fractal.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.NoRepositoryBean;

/**
 * Base repository interface for sharded domain entities.
 * <p>
 * All repositories managing entities co-located on physical shards should extend this interface.
 * Entities managed by a {@code ShardedRepository} must be annotated with
 * {@link io.github.mucchinas.fractal.annotation.ShardedRoot},
 * {@link io.github.mucchinas.fractal.annotation.ShardedEntity} (with a verified path to root),
 * or {@link io.github.mucchinas.fractal.annotation.ShardedReplica}.
 * <p>
 * Services annotated with {@link io.github.mucchinas.fractal.annotation.Sharded} are restricted
 * to injecting repositories that implement this interface.
 *
 * @param <T>  Domain entity type (must be @ShardedRoot, @ShardedEntity, or @ShardedReplica)
 * @param <ID> Entity identifier type
 */
@NoRepositoryBean
public interface ShardedRepository<T, ID> extends JpaRepository<T, ID> {
}
