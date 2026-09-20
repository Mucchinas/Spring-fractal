package it.neko.mukynas.fractal.annotation;

import java.lang.annotation.*;

/**
 * Marks a field or method as the partition key or foreign key used to determine shard placement.
 * <p>
 * On root entities (where {@link ShardedEntity#root()} is {@code true}), this identifies
 * the root partition identifier column.
 * <p>
 * On descendant entities, this identifies the foreign key relationship hopping back towards
 * the parent/root entity.
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardedKey {

    /**
     * Column name in this table representing the partition key or foreign key.
     * If blank, resolved from JPA @JoinColumn(name = "..."), @Column(name = "..."),
     * or the field name in snake_case.
     */
    String column() default "";

    /**
     * Target parent entity class that this key hops back to.
     * <p>
     * Optional if the annotated field type is already an entity reference (e.g. {@code private Project project}).
     * Required if the field is a scalar identifier (e.g. {@code private UUID projectId}).
     */
    Class<?> targetEntity() default Void.class;

    /**
     * Referenced column name in the target parent table.
     * If blank, defaults to the parent entity's primary key or @ShardedKey column.
     */
    String referencedColumn() default "";
}
