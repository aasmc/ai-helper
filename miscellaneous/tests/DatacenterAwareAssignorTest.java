package com.example.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Проверяет равномерное распределение активных партиций по ЦОД и подам.
 * Топология: 4 ЦОД × 6 подов × 3 консюмера = 72 члена, 36 партиций.
 * Ожидаем: по 9 партиций на ЦОД, каждый под непуст, 36 активных + 36 standby.
 *
 * ЦОД/под члена берутся ассайнером из userData; идентификатор члена кодируем как
 * "zone|pod|index" исключительно ради удобного извлечения ключей в проверках.
 */
class DatacenterAwareAssignorTest extends AbstractAssignorTest {

    @Test
    void distributesActivePartitionsEvenlyAcrossDatacentersAndPods() {
        int dcs = 4, podsPerDc = 6, consumersPerPod = 3, partitions = 36;

        DatacenterAwareAssignor assignor = eagerAssignor();

        Map<String, Subscription> subs = new HashMap<>();
        for (int d = 0; d < dcs; d++) {
            String dc = String.format("ns-dc%02d", d);
            for (int p = 0; p < podsPerDc; p++) {
                String pod = String.format("pod-%s-%02d", dc, p);
                for (int c = 0; c < consumersPerPod; c++) {
                    subs.put(pipeId(dc, pod, c), subscription(dc, pod));
                }
            }
        }
        assertEquals(72, subs.size());

        GroupAssignment ga = assignor.assign(cluster(partitions), new GroupSubscription(subs));
        Map<String, Set<TopicPartition>> map = toAssignmentMap(ga);

        // Каждый член присутствует в ответе.
        assertEquals(72, map.size());

        // Полное покрытие без пересечений.
        assertFullCoverageNoOverlap(map, partitions);

        // По 9 партиций на каждый ЦОД.
        Map<String, Integer> perDc = activeCountsByKey(map, AbstractAssignorTest::zoneFromPipeId);
        assertEquals(dcs, perDc.size(), "во всех ЦОД должны быть активные партиции");
        perDc.forEach((dc, n) -> assertEquals(9, n, "ЦОД " + dc + " должен иметь 9 партиций"));

        // Каждый под непуст (в ЦОД 9 партиций >= 6 подов).
        Map<String, Integer> perPod = activeCountsByKey(map, AbstractAssignorTest::podFromPipeId);
        assertEquals(dcs * podsPerDc, perPod.size(),
                "каждый под должен иметь хотя бы один активный консюмер");
        perPod.forEach((pod, n) -> assertTrue(n >= 1, "под " + pod + " не должен простаивать целиком"));

        // Ровно 36 активных членов, остальные — standby.
        assertEquals(partitions, activeMemberCount(map));
    }
}
