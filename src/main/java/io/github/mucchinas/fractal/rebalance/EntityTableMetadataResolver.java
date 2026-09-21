package io.github.mucchinas.fractal.rebalance;

import io.github.mucchinas.fractal.annotation.ShardedEntity;
import io.github.mucchinas.fractal.annotation.ShardedKey;
import io.github.mucchinas.fractal.annotation.ShardedReplica;
import io.github.mucchinas.fractal.annotation.ShardedRoot;
import io.github.mucchinas.fractal.annotation.ShardedStatus;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.*;

public class EntityTableMetadataResolver {

    private final ApplicationContext applicationContext;
    private volatile Class<?> resolvedRootClass;

    public EntityTableMetadataResolver() {
        this.applicationContext = null;
    }

    public EntityTableMetadataResolver(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public Class<?> getRootClass() {
        if (resolvedRootClass != null) {
            return resolvedRootClass;
        }
        resolve();
        return resolvedRootClass;
    }

    public void setResolvedRootClass(Class<?> rootClass) {
        this.resolvedRootClass = rootClass;
    }

    public EntityMetadataResult resolve() {
        Set<Class<?>> entityClasses = scanEntityClasses();
        if (entityClasses.isEmpty()) {
            return null;
        }
        return resolveFromClasses(entityClasses);
    }

    public EntityMetadataResult resolveFromClasses(Collection<Class<?>> classes) {
        if (classes == null || classes.isEmpty()) {
            return null;
        }

        List<Class<?>> replicaClasses = classes.stream()
                .filter(c -> c.isAnnotationPresent(ShardedReplica.class))
                .toList();

        List<String> replicaTables = new ArrayList<>();
        for (Class<?> clazz : replicaClasses) {
            replicaTables.add(resolveReplicaTableName(clazz));
        }

        List<Class<?>> shardedClasses = classes.stream()
                .filter(c -> c.isAnnotationPresent(ShardedEntity.class) || c.isAnnotationPresent(ShardedRoot.class))
                .toList();

        if (shardedClasses.isEmpty()) {
            if (!replicaTables.isEmpty()) {
                return new EntityMetadataResult(null, null, null, null, null, Collections.emptyList(), Collections.emptyList(), replicaTables);
            }
            return null;
        }

        Class<?> rootClass = null;
        for (Class<?> clazz : shardedClasses) {
            boolean isRoot = clazz.isAnnotationPresent(ShardedRoot.class) ||
                    (clazz.isAnnotationPresent(ShardedEntity.class) && clazz.getAnnotation(ShardedEntity.class).root());
            if (isRoot) {
                if (rootClass != null) {
                    throw new IllegalStateException("Multiple root entities found: "
                            + rootClass.getName() + " and " + clazz.getName() + ". Exactly one entity must be designated as root via @ShardedRoot.");
                }
                rootClass = clazz;
            }
        }

        if (rootClass == null) {
            throw new IllegalStateException("No root entity found among annotated classes. Exactly one entity must be designated as root via @ShardedRoot.");
        }

        this.resolvedRootClass = rootClass;

        String rootTable = resolveTableName(rootClass);
        String rootIdColumn = resolveKeyColumn(rootClass, true);

        Map<Class<?>, String> tableNames = new HashMap<>();
        for (Class<?> clazz : shardedClasses) {
            tableNames.put(clazz, resolveTableName(clazz));
        }

        List<TableForeignKey> foreignKeys = new ArrayList<>();
        Map<String, String> hopGraph = new HashMap<>();

        for (Class<?> clazz : shardedClasses) {
            if (clazz.equals(rootClass)) {
                continue;
            }

            KeyMemberInfo keyInfo = resolveKeyMember(clazz, false);
            if (keyInfo == null) {
                throw new IllegalStateException("Entity '" + clazz.getName() + "' is annotated with @ShardedEntity but has no @ShardedKey annotation.");
            }

            Class<?> targetClass = keyInfo.targetEntity();
            if (targetClass == null || targetClass.equals(Void.class)) {
                if (keyInfo.type() != null && (keyInfo.type().isAnnotationPresent(ShardedEntity.class) || keyInfo.type().isAnnotationPresent(ShardedRoot.class))) {
                    targetClass = keyInfo.type();
                } else {
                    throw new IllegalStateException("Cannot determine target parent entity for @ShardedKey on '"
                            + keyInfo.memberName() + "' in '" + clazz.getName() + "'. Specify targetEntity on @ShardedKey.");
                }
            }

            if (!shardedClasses.contains(targetClass)) {
                throw new IllegalStateException("Target parent entity '" + targetClass.getName()
                        + "' referenced by '" + clazz.getName() + "' is not an annotated @ShardedEntity or @ShardedRoot.");
            }

            String childTable = tableNames.get(clazz);
            String childColumn = keyInfo.columnName();
            String parentTable = tableNames.get(targetClass);
            String parentColumn = keyInfo.referencedColumn();

            if (parentColumn == null || parentColumn.isBlank()) {
                if (targetClass.equals(rootClass)) {
                    parentColumn = rootIdColumn;
                } else {
                    parentColumn = resolvePrimaryKeyColumn(targetClass);
                }
            }

            foreignKeys.add(new TableForeignKey(childTable, childColumn, parentTable, parentColumn));
            hopGraph.put(childTable, parentTable);
        }
        for (Class<?> clazz : shardedClasses) {
            if (!clazz.equals(rootClass) && hasShardedStatusAnnotation(clazz)) {
                throw new IllegalStateException("Entity '" + clazz.getName()
                        + "' declares @ShardedStatus but is not the root entity. @ShardedStatus is only permitted on the root entity annotated with @ShardedRoot.");
            }
        }
        for (Class<?> clazz : shardedClasses) {
            if (clazz.equals(rootClass)) {
                continue;
            }
            String current = tableNames.get(clazz);
            Set<String> visited = new HashSet<>();
            while (current != null && !current.equalsIgnoreCase(rootTable)) {
                if (!visited.add(current)) {
                    throw new IllegalStateException("Cycle detected in @ShardedKey relationship hierarchy involving table: " + current);
                }
                current = hopGraph.get(current);
            }
            if (current == null) {
                throw new IllegalStateException("Entity '" + clazz.getName() + "' (table '" + tableNames.get(clazz)
                        + "') has no path to root entity '" + rootClass.getName() + "'.");
            }
        }

        StatusMemberInfo statusInfo = resolveStatusMember(rootClass);
        String statusColumn = statusInfo != null ? statusInfo.columnName() : null;
        String migratingValue = statusInfo != null ? statusInfo.migratingValue() : null;
        String activeValue = statusInfo != null ? statusInfo.activeValue() : null;
        List<String> insertOrder = computeTopologicalOrder(rootTable, tableNames.values(), foreignKeys);

        return new EntityMetadataResult(rootTable, rootIdColumn, statusColumn, migratingValue, activeValue, insertOrder, foreignKeys, replicaTables);
    }

    private List<String> computeTopologicalOrder(String rootTable, Collection<String> allTables, List<TableForeignKey> fks) {
        Map<String, List<String>> graph = new HashMap<>();
        Map<String, Integer> inDegree = new HashMap<>();

        for (String table : allTables) {
            graph.put(table.toLowerCase(), new ArrayList<>());
            inDegree.put(table.toLowerCase(), 0);
        }

        for (TableForeignKey fk : fks) {
            String parent = fk.parentTable().toLowerCase();
            String child = fk.childTable().toLowerCase();
            if (graph.containsKey(parent) && graph.containsKey(child)) {
                if (!graph.get(parent).contains(child)) {
                    graph.get(parent).add(child);
                    inDegree.put(child, inDegree.get(child) + 1);
                }
            }
        }

        Queue<String> queue = new LinkedList<>();
        queue.offer(rootTable.toLowerCase());

        List<String> order = new ArrayList<>();
        while (!queue.isEmpty()) {
            String current = queue.poll();
            order.add(current);
            for (String neighbor : graph.getOrDefault(current, Collections.emptyList())) {
                inDegree.put(neighbor, inDegree.get(neighbor) - 1);
                if (inDegree.get(neighbor) == 0) {
                    queue.offer(neighbor);
                }
            }
        }
        for (String table : allTables) {
            String normalized = table.toLowerCase();
            if (!order.contains(normalized)) {
                order.add(normalized);
            }
        }

        return order;
    }

    public String resolveTableName(Class<?> clazz) {
        ShardedRoot rootAnn = clazz.getAnnotation(ShardedRoot.class);
        if (rootAnn != null && !rootAnn.table().isBlank()) {
            return rootAnn.table().toLowerCase();
        }
        ShardedEntity ann = clazz.getAnnotation(ShardedEntity.class);
        if (ann != null && !ann.table().isBlank()) {
            return ann.table().toLowerCase();
        }
        try {
            Class<?> tableAnnotationClass = Class.forName("jakarta.persistence.Table");
            if (clazz.isAnnotationPresent((Class<? extends Annotation>) tableAnnotationClass)) {
                Annotation tableAnn = clazz.getAnnotation((Class<? extends Annotation>) tableAnnotationClass);
                Method nameMethod = tableAnn.annotationType().getMethod("name");
                String name = (String) nameMethod.invoke(tableAnn);
                if (name != null && !name.isBlank()) {
                    return name.toLowerCase();
                }
            }
        } catch (Throwable ignored) {
        }

        return camelToSnakeCase(clazz.getSimpleName()).toLowerCase();
    }

    public String resolveReplicaTableName(Class<?> clazz) {
        ShardedReplica ann = clazz.getAnnotation(ShardedReplica.class);
        if (ann != null && !ann.table().isBlank()) {
            return ann.table().toLowerCase();
        }
        try {
            Class<?> tableAnnotationClass = Class.forName("jakarta.persistence.Table");
            if (clazz.isAnnotationPresent((Class<? extends Annotation>) tableAnnotationClass)) {
                Annotation tableAnn = clazz.getAnnotation((Class<? extends Annotation>) tableAnnotationClass);
                Method nameMethod = tableAnn.annotationType().getMethod("name");
                String name = (String) nameMethod.invoke(tableAnn);
                if (name != null && !name.isBlank()) {
                    return name.toLowerCase();
                }
            }
        } catch (Throwable ignored) {
        }

        return camelToSnakeCase(clazz.getSimpleName()).toLowerCase();
    }

    private String resolveKeyColumn(Class<?> clazz, boolean isRoot) {
        KeyMemberInfo info = resolveKeyMember(clazz, isRoot);
        if (info != null && info.columnName() != null && !info.columnName().isBlank()) {
            return info.columnName().toLowerCase();
        }
        return "id";
    }

    private String resolvePrimaryKeyColumn(Class<?> clazz) {
        Class<?> current = clazz;
        while (current != null && !current.equals(Object.class)) {
            for (Field field : current.getDeclaredFields()) {
                if (hasJpaIdAnnotation(field)) {
                    return resolveColumnFromMember(field, "");
                }
            }
            for (Method method : current.getDeclaredMethods()) {
                if (hasJpaIdAnnotation(method)) {
                    return resolveColumnFromMember(method, "");
                }
            }
            current = current.getSuperclass();
        }
        try {
            Field idField = clazz.getDeclaredField("id");
            return resolveColumnFromMember(idField, "");
        } catch (NoSuchFieldException ignored) {
        }
        return "id";
    }

    private KeyMemberInfo resolveKeyMember(Class<?> clazz, boolean isRoot) {
        Class<?> current = clazz;
        while (current != null && !current.equals(Object.class)) {
            for (Field field : current.getDeclaredFields()) {
                if (field.isAnnotationPresent(ShardedKey.class)) {
                    ShardedKey key = field.getAnnotation(ShardedKey.class);
                    String col = resolveColumnFromMember(field, key.column());
                    return new KeyMemberInfo(field.getName(), col, key.targetEntity(), key.referencedColumn(), field.getType());
                }
                if (isRoot && hasJpaIdAnnotation(field)) {
                    String col = resolveColumnFromMember(field, "");
                    return new KeyMemberInfo(field.getName(), col, Void.class, "", field.getType());
                }
            }
            for (Method method : current.getDeclaredMethods()) {
                if (method.isAnnotationPresent(ShardedKey.class)) {
                    ShardedKey key = method.getAnnotation(ShardedKey.class);
                    String col = resolveColumnFromMember(method, key.column());
                    return new KeyMemberInfo(method.getName(), col, key.targetEntity(), key.referencedColumn(), method.getReturnType());
                }
                if (isRoot && hasJpaIdAnnotation(method)) {
                    String col = resolveColumnFromMember(method, "");
                    return new KeyMemberInfo(method.getName(), col, Void.class, "", method.getReturnType());
                }
            }
            current = current.getSuperclass();
        }
        return null;
    }

    private String resolveColumnFromMember(Member member, String explicitColumn) {
        if (explicitColumn != null && !explicitColumn.isBlank()) {
            return explicitColumn.toLowerCase();
        }
        try {
            Class<?> joinColClass = Class.forName("jakarta.persistence.JoinColumn");
            if (isAnnotationPresent(member, (Class<? extends Annotation>) joinColClass)) {
                Annotation ann = getAnnotation(member, (Class<? extends Annotation>) joinColClass);
                String name = (String) ann.annotationType().getMethod("name").invoke(ann);
                if (name != null && !name.isBlank()) {
                    return name.toLowerCase();
                }
            }
        } catch (Throwable ignored) {
        }
        try {
            Class<?> colClass = Class.forName("jakarta.persistence.Column");
            if (isAnnotationPresent(member, (Class<? extends Annotation>) colClass)) {
                Annotation ann = getAnnotation(member, (Class<? extends Annotation>) colClass);
                String name = (String) ann.annotationType().getMethod("name").invoke(ann);
                if (name != null && !name.isBlank()) {
                    return name.toLowerCase();
                }
            }
        } catch (Throwable ignored) {
        }

        String name = member.getName();
        if (name.startsWith("get") && name.length() > 3) {
            name = Character.toLowerCase(name.charAt(3)) + name.substring(4);
        }
        return camelToSnakeCase(name).toLowerCase();
    }

    private boolean hasJpaIdAnnotation(Member member) {
        try {
            Class<?> idClass = Class.forName("jakarta.persistence.Id");
            return isAnnotationPresent(member, (Class<? extends Annotation>) idClass);
        } catch (Throwable ignored) {
            return false;
        }
    }

    private boolean isAnnotationPresent(Member member, Class<? extends Annotation> annotationClass) {
        if (member instanceof Field f) {
            return f.isAnnotationPresent(annotationClass);
        } else if (member instanceof Method m) {
            return m.isAnnotationPresent(annotationClass);
        }
        return false;
    }

    private Annotation getAnnotation(Member member, Class<? extends Annotation> annotationClass) {
        if (member instanceof Field f) {
            return f.getAnnotation(annotationClass);
        } else if (member instanceof Method m) {
            return m.getAnnotation(annotationClass);
        }
        return null;
    }

    public static String camelToSnakeCase(String str) {
        if (str == null || str.isBlank()) {
            return str;
        }
        return str.replaceAll("([a-z])([A-Z]+)", "$1_$2").toLowerCase();
    }

    private Set<Class<?>> scanEntityClasses() {
        if (applicationContext == null) {
            return Collections.emptySet();
        }

        List<String> packages = new ArrayList<>();
        try {
            if (AutoConfigurationPackages.has(applicationContext)) {
                packages.addAll(AutoConfigurationPackages.get(applicationContext));
            }
        } catch (Exception ignored) {
        }

        if (packages.isEmpty()) {
            Map<String, Object> annotatedBeans = applicationContext.getBeansWithAnnotation(
                    org.springframework.boot.autoconfigure.SpringBootApplication.class
            );
            for (Object bean : annotatedBeans.values()) {
                packages.add(ClassUtils.getPackageName(bean.getClass()));
            }
        }

        if (packages.isEmpty()) {
            return Collections.emptySet();
        }

        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(ShardedRoot.class));
        scanner.addIncludeFilter(new AnnotationTypeFilter(ShardedEntity.class));
        scanner.addIncludeFilter(new AnnotationTypeFilter(ShardedReplica.class));

        Set<Class<?>> classes = new HashSet<>();
        for (String basePackage : packages) {
            for (BeanDefinition bd : scanner.findCandidateComponents(basePackage)) {
                try {
                    Class<?> clazz = ClassUtils.forName(bd.getBeanClassName(), applicationContext.getClassLoader());
                    if (!clazz.isMemberClass() && !clazz.isAnonymousClass()) {
                        classes.add(clazz);
                    }
                } catch (ClassNotFoundException ignored) {
                }
            }
        }
        return classes;
    }

    private record KeyMemberInfo(
            String memberName,
            String columnName,
            Class<?> targetEntity,
            String referencedColumn,
            Class<?> type
    ) {}

    private boolean hasShardedStatusAnnotation(Class<?> clazz) {
        return resolveStatusMember(clazz) != null;
    }

    private StatusMemberInfo resolveStatusMember(Class<?> clazz) {
        Class<?> current = clazz;
        StatusMemberInfo found = null;
        while (current != null && !current.equals(Object.class)) {
            for (Field field : current.getDeclaredFields()) {
                if (field.isAnnotationPresent(ShardedStatus.class)) {
                    if (found != null) {
                        throw new IllegalStateException("Multiple @ShardedStatus annotations found on entity '"
                                + clazz.getName() + "'. Exactly one status column is supported.");
                    }
                    ShardedStatus status = field.getAnnotation(ShardedStatus.class);
                    String col = resolveColumnFromMember(field, status.column());
                    String migrating = status.migratingValue().isBlank() ? null : status.migratingValue();
                    String active = status.activeValue().isBlank() ? null : status.activeValue();
                    found = new StatusMemberInfo(col, migrating, active);
                }
            }
            for (Method method : current.getDeclaredMethods()) {
                if (method.isAnnotationPresent(ShardedStatus.class)) {
                    if (found != null) {
                        throw new IllegalStateException("Multiple @ShardedStatus annotations found on entity '"
                                + clazz.getName() + "'. Exactly one status column is supported.");
                    }
                    ShardedStatus status = method.getAnnotation(ShardedStatus.class);
                    String col = resolveColumnFromMember(method, status.column());
                    String migrating = status.migratingValue().isBlank() ? null : status.migratingValue();
                    String active = status.activeValue().isBlank() ? null : status.activeValue();
                    found = new StatusMemberInfo(col, migrating, active);
                }
            }
            current = current.getSuperclass();
        }
        return found;
    }

    private record StatusMemberInfo(
            String columnName,
            String migratingValue,
            String activeValue
    ) {}
}
