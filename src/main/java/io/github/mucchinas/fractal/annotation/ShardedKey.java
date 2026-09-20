package io.github.mucchinas.fractal.annotation;

import java.lang.annotation.*;

/**
 * Designates a partition key on a root entity, or a foreign key relationship hop
 * on a dependent entity within a sharded database cluster.
 * <p>
 * When placed on an entity annotated with {@link ShardedRoot}, this annotation marks the primary
 * partition key (e.g. tenant ID, organization ID, account ID) used to route queries to physical shards.
 * <p>
 * When placed on an entity annotated with {@link ShardedEntity}, this annotation defines how this
 * dependent entity navigates (hops) back to its parent entity in the cluster dependency graph.
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedKey {

    /**
     * The database column name in <b>THIS entity's table</b> (the child table) that holds
     * the foreign key pointing to the parent.
     * <p>
     * <b>Optional:</b> If omitted or empty, Fractal automatically infers the column name from:
     * <ol>
     *   <li>JPA {@code @JoinColumn(name = "...")} on this member</li>
     *   <li>JPA {@code @Column(name = "...")} on this member</li>
     *   <li>The snake_case conversion of the member's Java field name (e.g. {@code projectId} &rarr; {@code project_id})</li>
     * </ol>
     * You only need to provide this attribute if your physical database column name differs from
     * standard JPA annotations or conventions.
     */
    String column() default "";

    /**
     * The parent entity class in the sharding hierarchy that this foreign key references.
     * <p>
     * <b>Mandatory for scalar types</b> (such as {@link java.util.UUID}, {@link Long}, {@link String}):
     * When an entity models foreign keys as scalar IDs instead of full JPA entity associations,
     * Java reflection only sees {@code UUID} or {@code Long}. You must specify {@code targetEntity}
     * so Fractal knows which parent table to join against during rebalancing.
     * <p>
     * <b>Optional for JPA associations</b> (such as {@code @ManyToOne Organization org}):
     * If omitted, Fractal automatically infers the target entity from the field's declared type.
     */
    Class<?> targetEntity() default Void.class;

    /**
     * The column name in the <b>PARENT entity's table</b> that this foreign key joins against.
     * <p>
     * <b>Optional:</b> If omitted or empty, Fractal automatically defaults to the primary key
     * column (annotated with {@code @Id}) of the {@link #targetEntity()} (or the root partition column
     * if the parent is {@link ShardedRoot}).
     * <p>
     * You only need to specify this attribute in rare cases where the foreign key references a
     * non-primary-key unique column in the parent table.
     */
    String referencedColumn() default "";
}

