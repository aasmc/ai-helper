package com.example.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Интеграционный тест ассайнера на EmbeddedKafka (реальный брокер в JVM, реальный
 * ConsumerCoordinator и реальный протокол ребалансировки).
 *
 * Топология: 2 ЦОД × 2 пода × 2 консюмера = 8 членов группы, топик из 6 партиций.
 * Ожидаемое стабильное состояние: по 3 партиции на ЦОД, каждый под непуст,
 * 6 активных + 2 standby. После ухода одного консюмера из "dc-a" инварианты сохраняются.
 * Прогоняется для протоколов eager и cooperative.
 *
 * Общая обвязка проверок унаследована от {@link AbstractAssignorTest}.
 * ЦОД/под задаются per-consumer через consumer-property оверрайды (в одном JVM env общий);
 * в проде те же значения приходят из NAMESPACE / POD_SOURCE.
 */
@SpringJUnitConfig(EmbeddedKafkaAssignorIntegrationTest.TestConfig.class)
@EmbeddedKafka(
        topics = EmbeddedKafkaAssignorIntegrationTest.TOPIC,
        partitions = EmbeddedKafkaAssignorIntegrationTest.PARTITIONS,
        brokerProperties = {
                "offsets.topic.replication.factor=1",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "group.min.session.timeout.ms=4000"
        })
class EmbeddedKafkaAssignorIntegrationTest extends AbstractAssignorTest {

    static final String TOPIC = "events";
    static final int PARTITIONS = 6;
    private static final String GROUP_ID = "events-consumer-group-it";

    @Configuration
    static class TestConfig {
        // Пустой контекст: @EmbeddedKafka сам регистрирует EmbeddedKafkaBroker как бин.
    }

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private final List<ConsumerHarness> harnesses = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (ConsumerHarness h : harnesses) {
            h.stop();
        }
        harnesses.clear();
    }

    @ParameterizedTest(name = "protocol={0}")
    @ValueSource(strings = {"eager", "cooperative"})
    void balancesAcrossDatacentersAndSurvivesConsumerLeave(String protocol) throws Exception {
        String bootstrap = embeddedKafka.getBrokersAsString();

        // 2 ЦОД × 2 пода × 2 консюмера = 8 членов. id членов — в формате "zone|pod|index".
        for (String dc : List.of("dc-a", "dc-b")) {
            for (String pod : List.of(dc + "-pod-1", dc + "-pod-2")) {
                for (int c = 0; c < 2; c++) {
                    ConsumerHarness h = new ConsumerHarness(bootstrap, dc, pod, c, protocol);
                    harnesses.add(h);
                    h.start();
                }
            }
        }

        // 1) Ждём стабилизации и проверяем инварианты.
        Map<String, Set<TopicPartition>> stable = awaitStable(PARTITIONS, Duration.ofSeconds(45));
        assertInvariants(stable, "начальное распределение");
        assertEquals(PARTITIONS, activeMemberCount(stable),
                "должно быть 6 активных консюмеров (остальные — standby)");

        // 2) Уводим один консюмер из dc-a (graceful close -> LeaveGroup).
        ConsumerHarness leaving = harnesses.stream()
                .filter(h -> h.zone.equals("dc-a"))
                .findFirst()
                .orElseThrow();
        leaving.stop();
        harnesses.remove(leaving);

        // 3) Ждём повторной стабилизации и снова проверяем инварианты.
        Map<String, Set<TopicPartition>> afterLeave = awaitStable(PARTITIONS, Duration.ofSeconds(45));
        assertInvariants(afterLeave, "распределение после ухода консюмера из dc-a");
    }

    // ---------- Инварианты (делегируют базовым хелперам) ----------

    private void assertInvariants(Map<String, Set<TopicPartition>> assignment, String phase) {
        assertFullCoverageNoOverlap(assignment, PARTITIONS);
        Map<String, Integer> perZone = activeCountsByKey(assignment, AbstractAssignorTest::zoneFromPipeId);
        assertEquals(2, perZone.size(), phase + ": активные партиции должны быть в обоих ЦОД");
        perZone.forEach((zone, n) -> assertEquals(PARTITIONS / 2, n,
                phase + ": ЦОД " + zone + " должен иметь " + (PARTITIONS / 2) + " партиции"));
    }

    // ---------- Ожидание стабилизации ----------

    private Map<String, Set<TopicPartition>> awaitStable(int expectedTotal, Duration timeout)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        String prevSignature = null;
        int stableStreak = 0;

        while (System.currentTimeMillis() < deadline) {
            Map<String, Set<TopicPartition>> snap = currentSnapshot();
            int total = snap.values().stream().mapToInt(Set::size).sum();
            Set<TopicPartition> union = new HashSet<>();
            snap.values().forEach(union::addAll);

            String signature = signatureOf(snap);
            boolean fullyCovered = union.size() == expectedTotal && total == expectedTotal;

            if (fullyCovered && signature.equals(prevSignature)) {
                if (++stableStreak >= 3) {
                    return snap;
                }
            } else {
                stableStreak = 0;
            }
            prevSignature = signature;
            Thread.sleep(700);
        }
        fail("Группа не стабилизировалась за " + timeout.toSeconds() + " c. Снимок: " + currentSnapshot());
        return null; // недостижимо
    }

    private Map<String, Set<TopicPartition>> currentSnapshot() {
        Map<String, Set<TopicPartition>> copy = new HashMap<>();
        for (ConsumerHarness h : harnesses) {
            copy.put(h.id, new HashSet<>(h.currentAssignment()));
        }
        return copy;
    }

    private static String signatureOf(Map<String, Set<TopicPartition>> snap) {
        TreeMap<String, String> sorted = new TreeMap<>();
        for (Map.Entry<String, Set<TopicPartition>> e : snap.entrySet()) {
            List<String> tps = new ArrayList<>();
            for (TopicPartition tp : e.getValue()) {
                tps.add(tp.topic() + "-" + tp.partition());
            }
            Collections.sort(tps);
            sorted.put(e.getKey(), String.join(",", tps));
        }
        return sorted.toString();
    }

    // ---------- Обёртка над реальным KafkaConsumer в отдельном потоке ----------

    private final class ConsumerHarness {
        final String zone;
        final String pod;
        final String id; // "zone|pod|index" — согласуется с zoneFromPipeId/podFromPipeId
        private final KafkaConsumer<byte[], byte[]> consumer;
        private final Thread thread;
        private volatile boolean running = true;
        private volatile Set<TopicPartition> assignment = Collections.emptySet();

        ConsumerHarness(String bootstrap, String zone, String pod, int index, String protocol) {
            this.zone = zone;
            this.pod = pod;
            this.id = pipeId(zone, pod, index);
            this.consumer = new KafkaConsumer<>(consumerProps(bootstrap, zone, pod, protocol));
            this.thread = new Thread(this::runLoop, "consumer-" + id);
        }

        void start() {
            thread.start();
        }

        private void runLoop() {
            try {
                consumer.subscribe(List.of(TOPIC));
                while (running) {
                    consumer.poll(Duration.ofMillis(300));
                    this.assignment = new HashSet<>(consumer.assignment());
                }
            } catch (WakeupException expectedOnStop) {
                // штатное завершение
            } catch (RuntimeException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    consumer.close(Duration.ofSeconds(5)); // graceful -> LeaveGroup
                } catch (RuntimeException ignore) {
                    // already closed
                }
                this.assignment = Collections.emptySet();
            }
        }

        Set<TopicPartition> currentAssignment() {
            return assignment;
        }

        void stop() {
            running = false;
            consumer.wakeup();
            try {
                thread.join(Duration.ofSeconds(10).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static Properties consumerProps(String bootstrap, String zone, String pod, String protocol) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                DatacenterAwareAssignor.class.getName());
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
        p.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 2000);
        p.put(DatacenterAwareAssignor.ZONE_OVERRIDE_CONFIG, zone);
        p.put(DatacenterAwareAssignor.POD_OVERRIDE_CONFIG, pod);
        p.put(DatacenterAwareAssignor.REBALANCE_PROTOCOL_CONFIG, protocol);
        return p;
    }
}
