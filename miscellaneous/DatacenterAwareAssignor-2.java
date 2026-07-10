package com.example.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Кастомный ассайнер партиций: распределяет активные партиции равномерно
 * сначала по ЦОД, затем по подам внутри ЦОД, затем по консюмерам внутри пода.
 *
 * <p>ЦОД и под каждый консюмер сообщает лидеру через userData ({@link #subscriptionUserData(Set)}).
 * ЦОД берётся из env NAMESPACE, под — из env POD_SOURCE (см. конструктор).
 *
 * <p><b>Конфигурируемый протокол ребалансировки.</b> Через consumer-property
 * {@value #REBALANCE_PROTOCOL_CONFIG} можно выбрать {@code eager} (по умолчанию)
 * или {@code cooperative}. Значение управляет как {@link #supportedProtocols()}, так и
 * поведением {@link #assign}.
 *
 * <p>Контракт COOPERATIVE (Javadoc RebalanceProtocol,
 * https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/ConsumerPartitionAssignor.RebalanceProtocol.html):
 * «The assignor should not reassign any owned partitions immediately, but instead may indicate
 * consumers the need for partition revocation so that the revoked partitions can be reassigned
 * to other consumers in the next rebalance event.» Поэтому в кооперативном режиме мы НЕ отдаём
 * партицию новому владельцу, пока её удерживает старый: в этом раунде партиция не выдаётся никому,
 * старый владелец её отзывает, а в следующем ребалансе она достаётся целевому владельцу.
 *
 * <p>Один и тот же код безопасен в обоих режимах: при EAGER консюмеры отзывают все свои партиции
 * до вступления в группу, поэтому {@code ownedPartitions} приходит пустым и «удержаний» не возникает.
 */
public class DatacenterAwareAssignor implements ConsumerPartitionAssignor, Configurable {

    /** Версия схемы userData — на случай будущей эволюции формата метаданных. */
    private static final short USER_DATA_VERSION = 1;

    /** Consumer-property: выбор протокола ребалансировки: "eager" (по умолчанию) или "cooperative". */
    public static final String REBALANCE_PROTOCOL_CONFIG = "datacenter.assignor.rebalance.protocol";
    /** Consumer-property: переопределение зоны (ЦОД), если env NAMESPACE не подходит. */
    public static final String ZONE_OVERRIDE_CONFIG = "datacenter.assignor.zone";
    /** Consumer-property: переопределение имени пода, если env POD_SOURCE не подходит. */
    public static final String POD_OVERRIDE_CONFIG = "datacenter.assignor.pod";

    private final String zoneFromEnv; // ЦОД этого консюмера (из env)
    private final String podFromEnv;  // под этого консюмера (из env)

    private String resolvedZone;
    private String resolvedPod;
    private RebalanceProtocol protocol = RebalanceProtocol.EAGER; // по умолчанию

    /** Обязательный конструктор без аргументов: Kafka создаёт ассайнер через рефлексию. */
    public DatacenterAwareAssignor() {
        // >>> ТОЧКА НАСТРОЙКИ ЗОНЫ <<<
        // По условию: ЦОД из NAMESPACE, под из POD_SOURCE.
        // Корректно, ТОЛЬКО если имя namespace различается в разных кластерах OpenShift.
        // Если namespace одинаковый во всех ЦОД — задайте зону через ZONE_OVERRIDE_CONFIG
        // или поменяйте источник ниже на отдельную переменную (например System.getenv("DC")).
        this.zoneFromEnv = firstNonBlank(System.getenv("NAMESPACE"), "unknown-zone");
        this.podFromEnv = firstNonBlank(System.getenv("POD_SOURCE"), System.getenv("HOSTNAME"), "unknown-pod");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object zoneOverride = configs.get(ZONE_OVERRIDE_CONFIG);
        Object podOverride = configs.get(POD_OVERRIDE_CONFIG);
        this.resolvedZone = zoneOverride != null ? String.valueOf(zoneOverride) : this.zoneFromEnv;
        this.resolvedPod = podOverride != null ? String.valueOf(podOverride) : this.podFromEnv;

        Object protoRaw = configs.get(REBALANCE_PROTOCOL_CONFIG);
        this.protocol = parseProtocol(protoRaw);
    }

    private static RebalanceProtocol parseProtocol(Object raw) {
        if (raw == null) {
            return RebalanceProtocol.EAGER;
        }
        String v = String.valueOf(raw).trim().toLowerCase(Locale.ROOT);
        switch (v) {
            case "cooperative":
                return RebalanceProtocol.COOPERATIVE;
            case "eager":
            case "":
                return RebalanceProtocol.EAGER;
            default:
                throw new IllegalArgumentException(
                        REBALANCE_PROTOCOL_CONFIG + " must be 'eager' or 'cooperative', got: " + raw);
        }
    }

    private String zone() {
        return resolvedZone != null ? resolvedZone : zoneFromEnv;
    }

    private String pod() {
        return resolvedPod != null ? resolvedPod : podFromEnv;
    }

    @Override
    public String name() {
        // Все члены группы должны договориться об общем ассайнере по имени.
        return "datacenter-aware";
    }

    @Override
    public short version() {
        return USER_DATA_VERSION;
    }

    /**
     * Заявляем поддерживаемые протоколы согласно конфигу.
     * В кооперативном режиме заявляем [COOPERATIVE, EAGER]: порядок = приоритет, но EAGER
     * оставляем как фолбэк, если в группе есть члены, поддерживающие только EAGER.
     * В eager-режиме — только [EAGER].
     */
    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        if (protocol == RebalanceProtocol.COOPERATIVE) {
            return List.of(RebalanceProtocol.COOPERATIVE, RebalanceProtocol.EAGER);
        }
        return List.of(RebalanceProtocol.EAGER);
    }

    /** Кодируем (ЦОД, под) этого консюмера в userData, чтобы лидер увидел их при назначении. */
    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return encode(zone(), pod());
    }

    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();

        // 1) Разбираем метаданные членов и собираем текущее владение партициями.
        Map<String, MemberMeta> members = new HashMap<>();
        Map<TopicPartition, String> currentOwner = new HashMap<>(); // партиция -> текущий владелец
        for (Map.Entry<String, Subscription> e : subscriptions.entrySet()) {
            String memberId = e.getKey();
            Subscription sub = e.getValue();
            String[] zonePod = decode(sub.userData());
            String zone = zonePod[0].isEmpty() ? "unknown-zone" : zonePod[0];
            String pod = zonePod[1].isEmpty() ? memberId : zonePod[1];
            members.put(memberId, new MemberMeta(memberId, zone, pod, sub.topics()));
            // ownedPartitions заполнено только при COOPERATIVE; при EAGER — пусто.
            for (TopicPartition tp : sub.ownedPartitions()) {
                currentOwner.put(tp, memberId);
            }
        }

        // 2) Результат: каждый член обязан присутствовать (возможно, с пустым списком).
        Map<String, List<TopicPartition>> targetAssignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            targetAssignment.put(memberId, new ArrayList<>());
        }

        // 3) Детерминированный порядок членов: вложенный round-robin ЦОД -> под -> консюмер.
        List<String> globalOrder = buildInterleavedOrder(members);

        // 4) Все топики, на которые кто-либо подписан.
        Set<String> subscribedTopics = new TreeSet<>();
        for (MemberMeta m : members.values()) {
            subscribedTopics.addAll(m.topics);
        }

        // 5) Считаем ЦЕЛЕВОЕ (желаемое) распределение — как если бы протокол был EAGER.
        for (String topic : subscribedTopics) {
            Integer partitionCount = metadata.partitionCountForTopic(topic);
            if (partitionCount == null || partitionCount <= 0) {
                continue;
            }
            List<String> eligible = new ArrayList<>(globalOrder.size());
            for (String memberId : globalOrder) {
                MemberMeta m = members.get(memberId);
                if (m != null && m.topics.contains(topic)) {
                    eligible.add(memberId);
                }
            }
            if (eligible.isEmpty()) {
                continue;
            }
            for (int p = 0; p < partitionCount; p++) {
                String memberId = eligible.get(p % eligible.size());
                targetAssignment.get(memberId).add(new TopicPartition(topic, p));
            }
        }

        // 6) Кооперативная корректировка: не отдаём партицию новому владельцу, пока её
        //    удерживает старый. Такую партицию в этом раунде не выдаём никому — старый владелец
        //    её отзовёт, и в следующем ребалансе она достанется целевому владельцу.
        //    При EAGER currentOwner пуст, поэтому цикл ничего не меняет (полное распределение).
        if (protocol == RebalanceProtocol.COOPERATIVE) {
            Map<String, List<TopicPartition>> adjusted = new HashMap<>();
            for (String memberId : subscriptions.keySet()) {
                adjusted.put(memberId, new ArrayList<>());
            }
            for (Map.Entry<String, List<TopicPartition>> e : targetAssignment.entrySet()) {
                String targetMember = e.getKey();
                for (TopicPartition tp : e.getValue()) {
                    String owner = currentOwner.get(tp);
                    if (owner == null || owner.equals(targetMember)) {
                        // партиция свободна или уже у целевого владельца -> отдаём сразу
                        adjusted.get(targetMember).add(tp);
                    }
                    // иначе: удерживается другим -> НЕ выдаём в этом раунде (будет отозвана)
                }
            }
            targetAssignment = adjusted;
        }

        // 7) Оборачиваем в GroupAssignment.
        Map<String, Assignment> result = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> e : targetAssignment.entrySet()) {
            result.put(e.getKey(), new Assignment(e.getValue()));
        }
        return new GroupAssignment(result);
    }

    /**
     * Порядок членов вложенным round-robin: по одному члену из каждого ЦОД (в порядке ЦОД),
     * внутри ЦОД — по одному из каждого пода, внутри пода — по одному консюмеру.
     * Гарантирует покрытие всех ЦОД и подов раньше, чем кто-то получит вторую партицию.
     */
    private List<String> buildInterleavedOrder(Map<String, MemberMeta> members) {
        TreeMap<String, TreeMap<String, List<String>>> tree = new TreeMap<>();
        for (MemberMeta m : members.values()) {
            tree.computeIfAbsent(m.zone, z -> new TreeMap<>())
                .computeIfAbsent(m.pod, p -> new ArrayList<>())
                .add(m.memberId);
        }
        for (TreeMap<String, List<String>> pods : tree.values()) {
            for (List<String> consumers : pods.values()) {
                consumers.sort(Comparator.naturalOrder());
            }
        }
        List<List<String>> perZoneOrders = new ArrayList<>();
        for (TreeMap<String, List<String>> pods : tree.values()) {
            perZoneOrders.add(roundRobinFlatten(new ArrayList<>(pods.values())));
        }
        return roundRobinFlatten(perZoneOrders);
    }

    /** Берёт index 0 из каждой группы, потом index 1 из каждой, и т.д. (интерливинг). */
    private static <T> List<T> roundRobinFlatten(List<List<T>> groups) {
        List<T> out = new ArrayList<>();
        int max = 0;
        for (List<T> g : groups) {
            max = Math.max(max, g.size());
        }
        for (int i = 0; i < max; i++) {
            for (List<T> g : groups) {
                if (i < g.size()) {
                    out.add(g.get(i));
                }
            }
        }
        return out;
    }

    // ---------- userData: [short version][int zoneLen][zone][int podLen][pod] ----------

    private static ByteBuffer encode(String zone, String pod) {
        byte[] z = zone.getBytes(StandardCharsets.UTF_8);
        byte[] p = pod.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + 4 + z.length + 4 + p.length);
        buf.putShort(USER_DATA_VERSION);
        buf.putInt(z.length);
        buf.put(z);
        buf.putInt(p.length);
        buf.put(p);
        buf.flip();
        return buf;
    }

    /** Возвращает {zone, pod}; при любой ошибке — пустые строки (сработают фолбэки в assign). */
    private static String[] decode(ByteBuffer userData) {
        if (userData == null || userData.remaining() < 2) {
            return new String[]{"", ""};
        }
        ByteBuffer buf = userData.duplicate();
        try {
            buf.getShort(); // version
            String zone = readString(buf);
            String pod = readString(buf);
            return new String[]{zone, pod};
        } catch (RuntimeException ex) {
            return new String[]{"", ""};
        }
    }

    private static String readString(ByteBuffer buf) {
        int len = buf.getInt();
        if (len < 0 || len > buf.remaining()) {
            throw new IllegalArgumentException("Bad userData length: " + len);
        }
        byte[] b = new byte[len];
        buf.get(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v;
            }
        }
        return "";
    }

    private static final class MemberMeta {
        final String memberId;
        final String zone;
        final String pod;
        final Set<String> topics;

        MemberMeta(String memberId, String zone, String pod, List<String> topics) {
            this.memberId = memberId;
            this.zone = zone;
            this.pod = pod;
            this.topics = new HashSet<>(topics);
        }
    }
}
