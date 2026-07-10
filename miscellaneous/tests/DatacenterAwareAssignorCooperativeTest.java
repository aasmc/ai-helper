package com.example.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

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
 */
class DatacenterAwareAssignorCooperativeTest extends AbstractAssignorTest {

    private static final TopicPartition TP0 = new TopicPartition(DEFAULT_TOPIC, 0);

    @Test
    void cooperativeMode_declaresCooperativeThenEager() {
        assertEquals(
                List.of(RebalanceProtocol.COOPERATIVE, RebalanceProtocol.EAGER),
                cooperativeAssignor().supportedProtocols());
    }

    @Test
    void eagerMode_declaresOnlyEager() {
        assertEquals(List.of(RebalanceProtocol.EAGER), eagerAssignor().supportedProtocols());
    }

    /**
     * Раунд 1: партицию, которая должна переехать от текущего владельца к другому,
     * ассайнер НЕ выдаёт новому владельцу (withholding) — она не выдаётся никому.
     * Раунд 2: когда партиция освобождена, она достаётся целевому владельцу.
     */
    @Test
    void movingPartition_isWithheldThenReassigned() {
        DatacenterAwareAssignor assignor = cooperativeAssignor();

        String memberA = "member-in-dcA";
        String memberB = "member-in-dcB";

        // Целевого владельца определяем чистым расчётом (без текущего владения).
        Map<String, Subscription> clean = new HashMap<>();
        clean.put(memberA, subscription("dcA", "podA"));
        clean.put(memberB, subscription("dcB", "podB"));
        GroupAssignment target = assignor.assign(cluster(1), new GroupSubscription(clean));
        String targetMember = ownership(target).get(TP0);
        assertEquals(1, ownership(target).size(), "в целевом расчёте партиция должна быть назначена");

        String otherMember = targetMember.equals(memberA) ? memberB : memberA;
        String zoneTarget = targetMember.equals(memberA) ? "dcA" : "dcB";
        String podTarget = targetMember.equals(memberA) ? "podA" : "podB";
        String zoneOther = otherMember.equals(memberA) ? "dcA" : "dcB";
        String podOther = otherMember.equals(memberA) ? "podA" : "podB";

        // РАУНД 1: партицию сейчас держит "не тот" консюмер (otherMember).
        Map<String, Subscription> round1 = new HashMap<>();
        round1.put(targetMember, subscription(zoneTarget, podTarget));
        round1.put(otherMember, subscription(zoneOther, podOther, List.of(TP0)));
        GroupAssignment r1 = assignor.assign(cluster(1), new GroupSubscription(round1));

        assertFalse(partitionsOf(r1, targetMember).contains(TP0),
                "новый владелец не должен получить партицию, пока её удерживает старый");
        assertFalse(partitionsOf(r1, otherMember).contains(TP0),
                "старый владелец не должен сохранить партицию (=> отзовёт её)");
        assertTrue(ownership(r1).isEmpty(), "в раунде 1 движущаяся партиция не выдаётся никому");

        // РАУНД 2: старый владелец отозвал партицию -> ownedPartitions пусты.
        Map<String, Subscription> round2 = new HashMap<>();
        round2.put(targetMember, subscription(zoneTarget, podTarget));
        round2.put(otherMember, subscription(zoneOther, podOther));
        GroupAssignment r2 = assignor.assign(cluster(1), new GroupSubscription(round2));

        assertTrue(partitionsOf(r2, targetMember).contains(TP0),
                "после освобождения партиция должна достаться целевому владельцу");
        assertFalse(partitionsOf(r2, otherMember).contains(TP0));
    }

    /** Партиция, уже принадлежащая целевому владельцу, остаётся у него в том же раунде. */
    @Test
    void alreadyOwnedByTargetMember_isKeptImmediately() {
        DatacenterAwareAssignor assignor = cooperativeAssignor();

        String memberA = "member-in-dcA";
        String memberB = "member-in-dcB";

        Map<String, Subscription> clean = new HashMap<>();
        clean.put(memberA, subscription("dcA", "podA"));
        clean.put(memberB, subscription("dcB", "podB"));
        String targetMember = ownership(assignor.assign(cluster(1), new GroupSubscription(clean))).get(TP0);

        String zoneTarget = targetMember.equals(memberA) ? "dcA" : "dcB";
        String podTarget = targetMember.equals(memberA) ? "podA" : "podB";
        String otherMember = targetMember.equals(memberA) ? memberB : memberA;
        String zoneOther = otherMember.equals(memberA) ? "dcA" : "dcB";
        String podOther = otherMember.equals(memberA) ? "podA" : "podB";

        Map<String, Subscription> subs = new HashMap<>();
        subs.put(targetMember, subscription(zoneTarget, podTarget, List.of(TP0)));
        subs.put(otherMember, subscription(zoneOther, podOther));
        GroupAssignment ga = assignor.assign(cluster(1), new GroupSubscription(subs));

        assertTrue(partitionsOf(ga, targetMember).contains(TP0),
                "партиция, уже принадлежащая целевому владельцу, должна остаться у него сразу");
    }

    /** В eager-режиме withholding не применяется: партиция выдаётся сразу. */
    @Test
    void eagerMode_assignsFullyWithoutWithholding() {
        DatacenterAwareAssignor assignor = eagerAssignor();

        Map<String, Subscription> subs = new HashMap<>();
        subs.put("member-in-dcA", subscription("dcA", "podA"));
        subs.put("member-in-dcB", subscription("dcB", "podB", List.of(TP0)));
        GroupAssignment ga = assignor.assign(cluster(1), new GroupSubscription(subs));

        assertEquals(1, ownership(ga).size(),
                "в eager-режиме партиция должна быть выдана сразу, без withholding");
    }
}
