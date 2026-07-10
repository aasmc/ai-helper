package com.example.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Тесты кооперативного протокола ребалансировки.
 *
 * Контракт (Javadoc RebalanceProtocol):
 * https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/ConsumerPartitionAssignor.RebalanceProtocol.html
 * — при COOPERATIVE ассайнер не должен немедленно переназначать уже принадлежащую партицию;
 *   он «показывает» старому владельцу необходимость отзыва (не выдавая партицию в этом раунде),
 *   а новому владельцу партиция достаётся в следующем ребалансе.
 *
 * Схема проверки движения A -> B за два раунда:
 *   РАУНД 1: партицию держит A (ownedPartitions=[tp]), но целевой владелец по расчёту — B.
 *            Ожидаем: B её НЕ получил, у A её тоже НЕТ в новом назначении => A её отзовёт.
 *   РАУНД 2: A больше не владелец (отзыв уже произошёл, ownedPartitions пусты у всех),
 *            партиция свободна. Ожидаем: B её получил.
 */
class DatacenterAwareAssignorCooperativeTest {

    private static final String TOPIC = "events";

    // ---- вспомогательное: userData в том же формате, что и ассайнер ----
    private static ByteBuffer userData(String zone, String pod) {
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

    private static Cluster clusterWithTopic(String topic, int partitions) {
        Node node = new Node(0, "localhost", 9092);
        List<PartitionInfo> pis = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            pis.add(new PartitionInfo(topic, i, node, new Node[]{node}, new Node[]{node}));
        }
        return new Cluster("test-cluster", List.of(node), pis,
                Collections.emptySet(), Collections.emptySet());
    }

    private DatacenterAwareAssignor cooperativeAssignor() {
        DatacenterAwareAssignor a = new DatacenterAwareAssignor();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(DatacenterAwareAssignor.REBALANCE_PROTOCOL_CONFIG, "cooperative");
        a.configure(cfg);
        return a;
    }

    private static List<TopicPartition> partitionsOf(GroupAssignment ga, String member) {
        Assignment a = ga.groupAssignment().get(member);
        return a == null ? List.of() : a.partitions();
    }

    /** Собирает, кто в итоге владеет каждой партицией (partition -> member). */
    private static Map<TopicPartition, String> ownership(GroupAssignment ga) {
        Map<TopicPartition, String> owners = new HashMap<>();
        for (Map.Entry<String, Assignment> e : ga.groupAssignment().entrySet()) {
            for (TopicPartition tp : e.getValue().partitions()) {
                owners.put(tp, e.getKey());
            }
        }
        return owners;
    }

    @Test
    void cooperativeMode_declaresCooperativeThenEager() {
        DatacenterAwareAssignor a = cooperativeAssignor();
        assertEquals(
                List.of(RebalanceProtocol.COOPERATIVE, RebalanceProtocol.EAGER),
                a.supportedProtocols());
    }

    @Test
    void eagerMode_declaresOnlyEager() {
        DatacenterAwareAssignor a = new DatacenterAwareAssignor();
        a.configure(new HashMap<>()); // без флага -> eager по умолчанию
        assertEquals(List.of(RebalanceProtocol.EAGER), a.supportedProtocols());
    }

    /**
     * Раунд 1: партицию, которая должна переехать от текущего владельца к другому,
     * ассайнер НЕ выдаёт новому владельцу в этом раунде (withholding).
     * Раунд 2: когда партиция освобождена, она достаётся целевому владельцу.
     *
     * Конструкция кейса: 2 ЦОД, в каждом 1 под, в каждом поде 1 консюмер (всего 2 консюмера)
     * и 1 партиция. Мы искусственно создаём ситуацию, где партицию сейчас держит "неправильный"
     * консюмер (тот, кому она по расчёту не полагается), и проверяем перенос за два раунда.
     */
    @Test
    void movingPartition_isWithheldThenReassigned() {
        DatacenterAwareAssignor assignor = cooperativeAssignor();
        Cluster cluster = clusterWithTopic(TOPIC, 1);
        TopicPartition tp0 = new TopicPartition(TOPIC, 0);

        // Два консюмера в разных ЦОД. Порядок ЦОД лексикографический: dcA < dcB,
        // поэтому единственная партиция по целевому расчёту достаётся консюмеру из dcA.
        String memberA = "member-in-dcA";
        String memberB = "member-in-dcB";

        // Сначала узнаем "целевого" владельца без всякого текущего владения (чистый расчёт),
        // чтобы тест не зависел от предположений о порядке.
        Map<String, Subscription> clean = new HashMap<>();
        clean.put(memberA, new Subscription(List.of(TOPIC), userData("dcA", "podA"), List.of()));
        clean.put(memberB, new Subscription(List.of(TOPIC), userData("dcB", "podB"), List.of()));
        GroupAssignment target = assignor.assign(cluster, new GroupSubscription(clean));
        Map<TopicPartition, String> targetOwners = ownership(target);
        String targetMember = targetOwners.get(tp0);
        String otherMember = targetMember.equals(memberA) ? memberB : memberA;
        // Санити: в чистом расчёте партиция кому-то досталась
        assertEquals(1, targetOwners.size(), "в целевом расчёте партиция должна быть назначена");

        // РАУНД 1: партицию СЕЙЧАС держит "не тот" консюмер (otherMember).
        Map<String, Subscription> round1 = new HashMap<>();
        String zoneOf = targetMember.equals(memberA) ? "dcA" : "dcB";
        String podOf = targetMember.equals(memberA) ? "podA" : "podB";
        String zoneOther = otherMember.equals(memberA) ? "dcA" : "dcB";
        String podOther = otherMember.equals(memberA) ? "podA" : "podB";
        round1.put(targetMember, new Subscription(List.of(TOPIC), userData(zoneOf, podOf), List.of()));
        round1.put(otherMember, new Subscription(List.of(TOPIC), userData(zoneOther, podOther), List.of(tp0)));

        GroupAssignment r1 = assignor.assign(cluster, new GroupSubscription(round1));

        // Ожидаем withholding: партицию не получил НИКТО в этом раунде.
        assertFalse(partitionsOf(r1, targetMember).contains(tp0),
                "новый владелец не должен получить партицию, пока её удерживает старый");
        assertFalse(partitionsOf(r1, otherMember).contains(tp0),
                "старый владелец не должен сохранить партицию в новом назначении (=> отзовёт её)");
        assertTrue(ownership(r1).isEmpty(),
                "в раунде 1 движущаяся партиция не выдаётся никому");

        // РАУНД 2: старый владелец отозвал партицию -> ownedPartitions пусты у всех.
        Map<String, Subscription> round2 = new HashMap<>();
        round2.put(targetMember, new Subscription(List.of(TOPIC), userData(zoneOf, podOf), List.of()));
        round2.put(otherMember, new Subscription(List.of(TOPIC), userData(zoneOther, podOther), List.of()));

        GroupAssignment r2 = assignor.assign(cluster, new GroupSubscription(round2));

        assertTrue(partitionsOf(r2, targetMember).contains(tp0),
                "после освобождения партиция должна достаться целевому владельцу");
        assertFalse(partitionsOf(r2, otherMember).contains(tp0));
    }

    /**
     * Если целевой владелец уже держит партицию, кооперативный режим её не трогает
     * (никаких лишних отзывов): партиция остаётся у него в этом же раунде.
     */
    @Test
    void alreadyOwnedByTargetMember_isKeptImmediately() {
        DatacenterAwareAssignor assignor = cooperativeAssignor();
        Cluster cluster = clusterWithTopic(TOPIC, 1);
        TopicPartition tp0 = new TopicPartition(TOPIC, 0);

        String memberA = "member-in-dcA";
        String memberB = "member-in-dcB";

        // Определяем целевого владельца чистым расчётом.
        Map<String, Subscription> clean = new HashMap<>();
        clean.put(memberA, new Subscription(List.of(TOPIC), userData("dcA", "podA"), List.of()));
        clean.put(memberB, new Subscription(List.of(TOPIC), userData("dcB", "podB"), List.of()));
        String targetMember = ownership(assignor.assign(cluster, new GroupSubscription(clean))).get(tp0);

        String zoneOf = targetMember.equals(memberA) ? "dcA" : "dcB";
        String podOf = targetMember.equals(memberA) ? "podA" : "podB";
        String otherMember = targetMember.equals(memberA) ? memberB : memberA;
        String zoneOther = otherMember.equals(memberA) ? "dcA" : "dcB";
        String podOther = otherMember.equals(memberA) ? "podA" : "podB";

        // Партицию УЖЕ держит целевой владелец.
        Map<String, Subscription> subs = new HashMap<>();
        subs.put(targetMember, new Subscription(List.of(TOPIC), userData(zoneOf, podOf), List.of(tp0)));
        subs.put(otherMember, new Subscription(List.of(TOPIC), userData(zoneOther, podOther), List.of()));

        GroupAssignment ga = assignor.assign(cluster, new GroupSubscription(subs));

        assertTrue(partitionsOf(ga, targetMember).contains(tp0),
                "партиция, уже принадлежащая целевому владельцу, должна остаться у него сразу");
    }

    /**
     * В eager-режиме тот же ассайнер выдаёт полное распределение сразу, даже если формально
     * передать ownedPartitions (в реальном EAGER они были бы пусты; здесь показываем, что
     * withholding под eager не применяется — блок кооперативной корректировки выключен).
     */
    @Test
    void eagerMode_assignsFullyWithoutWithholding() {
        DatacenterAwareAssignor assignor = new DatacenterAwareAssignor();
        assignor.configure(new HashMap<>()); // eager
        Cluster cluster = clusterWithTopic(TOPIC, 1);
        TopicPartition tp0 = new TopicPartition(TOPIC, 0);

        String memberA = "member-in-dcA";
        String memberB = "member-in-dcB";

        Map<String, Subscription> subs = new HashMap<>();
        // Партицию держит otherMember, но в eager это не влияет на выдачу.
        subs.put(memberA, new Subscription(List.of(TOPIC), userData("dcA", "podA"), List.of()));
        subs.put(memberB, new Subscription(List.of(TOPIC), userData("dcB", "podB"), List.of(tp0)));

        GroupAssignment ga = assignor.assign(cluster, new GroupSubscription(subs));

        // Партиция назначена (не удержана) ровно одному члену.
        assertEquals(1, ownership(ga).size(),
                "в eager-режиме партиция должна быть выдана сразу, без withholding");
    }
}
