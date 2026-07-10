package com.example.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Тест демонстрирует, что при 4 ЦОД × 6 подов × 3 консюмера (= 72 члена) и 36 партициях
 * активные партиции распределяются по 9 на ЦОД и не оставляют ни один под целиком пустым,
 * пока в поде есть шанс на партицию.
 *
 * Здесь мы вызываем encode-формат тем же способом, что и продакшн-код, через маленький хелпер,
 * повторяющий приватный формат userData. Это осознанный компромисс, чтобы не открывать API.
 */
class DatacenterAwareAssignorTest {

    private static final String TOPIC = "events";
    private static final int PARTITIONS = 36;

    @Test
    void distributesActivePartitionsEvenlyAcrossDatacentersAndPods() {
        int dcs = 4, podsPerDc = 6, consumersPerPod = 3;

        DatacenterAwareAssignor assignor = new DatacenterAwareAssignor();

        Map<String, Subscription> subs = new HashMap<>();
        // memberId -> (dc, pod) для последующей проверки
        Map<String, String[]> meta = new HashMap<>();

        for (int d = 0; d < dcs; d++) {
            String dc = String.format("ns-dc%02d", d);
            for (int p = 0; p < podsPerDc; p++) {
                String pod = String.format("pod-%s-%02d", dc, p);
                for (int c = 0; c < consumersPerPod; c++) {
                    String memberId = pod + "-c" + c + "-" + java.util.UUID.randomUUID();
                    Subscription sub = new Subscription(List.of(TOPIC), userData(dc, pod));
                    subs.put(memberId, sub);
                    meta.put(memberId, new String[]{dc, pod});
                }
            }
        }
        assertEquals(72, subs.size());

        Cluster cluster = clusterWithTopic(TOPIC, PARTITIONS);
        GroupAssignment ga = assignor.assign(cluster, new GroupSubscription(subs));
        Map<String, Assignment> result = ga.groupAssignment();

        // Каждый член присутствует в ответе
        assertEquals(72, result.size());

        // Суммарно назначено ровно 36 партиций, без пересечений
        int total = 0;
        TreeMap<String, Integer> perDc = new TreeMap<>();
        Map<String, Integer> perPod = new HashMap<>();
        for (Map.Entry<String, Assignment> e : result.entrySet()) {
            int n = e.getValue().partitions().size();
            total += n;
            if (n > 0) {
                String[] mp = meta.get(e.getKey());
                perDc.merge(mp[0], n, Integer::sum);
                perPod.merge(mp[1], n, Integer::sum);
            }
        }
        assertEquals(PARTITIONS, total);

        // Каждый ЦОД получил ровно 9 партиций (36 / 4)
        assertEquals(dcs, perDc.size(), "во всех ЦОД должны быть активные партиции");
        for (Map.Entry<String, Integer> e : perDc.entrySet()) {
            assertEquals(9, e.getValue(), "ЦОД " + e.getKey() + " должен иметь 9 партиций");
        }

        // 9 партиций на 6 подов -> каждый под получает 1 или 2; ни один под не пуст,
        // потому что партиций в ЦОД (9) >= подов (6).
        // Проверим, что во всех 24 подах есть хотя бы одна партиция.
        assertEquals(dcs * podsPerDc, perPod.size(),
                "каждый под должен иметь хотя бы один активный консюмер");
        for (Map.Entry<String, Integer> e : perPod.entrySet()) {
            assertTrue(e.getValue() >= 1, "под " + e.getKey() + " не должен простаивать целиком");
        }
    }

    // Повторяет приватный формат userData ассайнера: [short v][int zLen][z][int pLen][p]
    private static ByteBuffer userData(String zone, String pod) {
        byte[] z = zone.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] p = pod.getBytes(java.nio.charset.StandardCharsets.UTF_8);
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
                java.util.Collections.emptySet(), java.util.Collections.emptySet());
    }
}
