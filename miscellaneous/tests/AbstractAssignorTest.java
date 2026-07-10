package com.example.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Базовый класс для тестов {@link DatacenterAwareAssignor}: собирает всю общую обвязку,
 * чтобы юнит- и интеграционные тесты не дублировали код.
 *
 * <p>Содержит: фабрику ассайнера с выбором протокола, кодирование userData (тот же формат,
 * что и в ассайнере), билдеры {@code Subscription} и {@code Cluster}, адаптеры результата
 * {@code GroupAssignment} к простой карте {@code member -> partitions}, а также
 * параметризуемые проверки покрытия и баланса по произвольному ключу (ЦОД, под).
 *
 * <p>Класс абстрактный и не содержит тестовых методов, поэтому JUnit его не запускает.
 * Spring-аннотации ({@code @EmbeddedKafka} и т.п.) навешиваются на конкретные подклассы,
 * а не здесь, поэтому базовый класс одинаково подходит и обычным, и Spring-тестам.
 */
public abstract class AbstractAssignorTest {

    /** Топик по умолчанию для юнит-тестов и билдеров. */
    protected static final String DEFAULT_TOPIC = "events";

    // ------------------------------------------------------------------
    //  Фабрика ассайнера
    // ------------------------------------------------------------------

    /** Создаёт и конфигурирует ассайнер с заданным протоколом ("eager"/"cooperative"/null=eager). */
    protected DatacenterAwareAssignor assignor(String protocol) {
        DatacenterAwareAssignor a = new DatacenterAwareAssignor();
        Map<String, Object> cfg = new HashMap<>();
        if (protocol != null) {
            cfg.put(DatacenterAwareAssignor.REBALANCE_PROTOCOL_CONFIG, protocol);
        }
        a.configure(cfg);
        return a;
    }

    protected DatacenterAwareAssignor eagerAssignor() {
        return assignor("eager");
    }

    protected DatacenterAwareAssignor cooperativeAssignor() {
        return assignor("cooperative");
    }

    // ------------------------------------------------------------------
    //  userData: тот же формат, что кодирует/декодирует сам ассайнер
    //  [short version][int zoneLen][zone][int podLen][pod]
    // ------------------------------------------------------------------

    protected static ByteBuffer userData(String zone, String pod) {
        byte[] z = zone.getBytes(StandardCharsets.UTF_8);
        byte[] p = pod.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + 4 + z.length + 4 + p.length);
        buf.putShort((short) 1);
        buf.putInt(z.length);
        buf.put(z);
        buf.putInt(p.length);
        buf.put(p);
        buf.flip();
        return buf;
    }

    // ------------------------------------------------------------------
    //  Билдеры Subscription (топик по умолчанию — DEFAULT_TOPIC)
    // ------------------------------------------------------------------

    protected static Subscription subscription(String zone, String pod) {
        return subscription(zone, pod, List.of());
    }

    protected static Subscription subscription(String zone, String pod, List<TopicPartition> owned) {
        return new Subscription(List.of(DEFAULT_TOPIC), userData(zone, pod), owned);
    }

    // ------------------------------------------------------------------
    //  Cluster
    // ------------------------------------------------------------------

    protected static Cluster cluster(int partitions) {
        return clusterWithTopic(DEFAULT_TOPIC, partitions);
    }

    protected static Cluster clusterWithTopic(String topic, int partitions) {
        Node node = new Node(0, "localhost", 9092);
        List<PartitionInfo> pis = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            pis.add(new PartitionInfo(topic, i, node, new Node[]{node}, new Node[]{node}));
        }
        return new Cluster("test-cluster", List.of(node), pis,
                Collections.emptySet(), Collections.emptySet());
    }

    // ------------------------------------------------------------------
    //  Адаптеры результата GroupAssignment
    // ------------------------------------------------------------------

    /** Нормализует результат ассайнера к простой карте member -> набор партиций. */
    protected static Map<String, Set<TopicPartition>> toAssignmentMap(GroupAssignment ga) {
        Map<String, Set<TopicPartition>> m = new HashMap<>();
        for (Map.Entry<String, Assignment> e : ga.groupAssignment().entrySet()) {
            m.put(e.getKey(), new HashSet<>(e.getValue().partitions()));
        }
        return m;
    }

    protected static List<TopicPartition> partitionsOf(GroupAssignment ga, String member) {
        Assignment a = ga.groupAssignment().get(member);
        return a == null ? List.of() : a.partitions();
    }

    /** partition -> текущий владелец (из результата ассайнера). */
    protected static Map<TopicPartition, String> ownership(GroupAssignment ga) {
        return ownership(toAssignmentMap(ga));
    }

    /** partition -> владелец (из карты member -> partitions). */
    protected static Map<TopicPartition, String> ownership(Map<String, Set<TopicPartition>> assignment) {
        Map<TopicPartition, String> owners = new HashMap<>();
        for (Map.Entry<String, Set<TopicPartition>> e : assignment.entrySet()) {
            for (TopicPartition tp : e.getValue()) {
                owners.put(tp, e.getKey());
            }
        }
        return owners;
    }

    // ------------------------------------------------------------------
    //  Общие проверки
    // ------------------------------------------------------------------

    /** Все партиции покрыты ровно один раз (нет дыр и пересечений). */
    protected static void assertFullCoverageNoOverlap(Map<String, Set<TopicPartition>> assignment,
                                                      int expectedPartitions) {
        Set<TopicPartition> union = new HashSet<>();
        int total = 0;
        for (Set<TopicPartition> parts : assignment.values()) {
            union.addAll(parts);
            total += parts.size();
        }
        assertEquals(expectedPartitions, union.size(), "должны быть покрыты все партиции");
        assertEquals(expectedPartitions, total, "ни одна партиция не должна быть назначена дважды");
    }

    /**
     * Считает число активных партиций по произвольному ключу (например, ЦОД или под).
     * keyOf извлекает ключ из идентификатора члена группы. Standby-члены (пустые) игнорируются.
     */
    protected static Map<String, Integer> activeCountsByKey(Map<String, Set<TopicPartition>> assignment,
                                                            Function<String, String> keyOf) {
        TreeMap<String, Integer> counts = new TreeMap<>();
        for (Map.Entry<String, Set<TopicPartition>> e : assignment.entrySet()) {
            if (e.getValue().isEmpty()) {
                continue;
            }
            counts.merge(keyOf.apply(e.getKey()), e.getValue().size(), Integer::sum);
        }
        return counts;
    }

    /** Количество активных (непустых) членов. */
    protected static long activeMemberCount(Map<String, Set<TopicPartition>> assignment) {
        return assignment.values().stream().filter(s -> !s.isEmpty()).count();
    }

    // ------------------------------------------------------------------
    //  Идентификаторы вида "zone|pod|index" и их разбор
    //  (используются, чтобы извлекать ЦОД/под из id члена без отдельной карты)
    // ------------------------------------------------------------------

    protected static String pipeId(String zone, String pod, int index) {
        return zone + "|" + pod + "|" + index;
    }

    protected static String zoneFromPipeId(String id) {
        return id.split("\\|", 2)[0];
    }

    protected static String podFromPipeId(String id) {
        return id.split("\\|", 3)[1];
    }
}
