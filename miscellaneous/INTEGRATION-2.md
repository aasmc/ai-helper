# Подключение `DatacenterAwareAssignor` в Spring Boot 4 + OpenShift

## 1. Как работает ассайнер (кратко)

- Каждый консюмер при подписке кладёт свои `(ЦОД, под)` в `userData` (метод `subscriptionUserData`). ЦОД берётся из env `NAMESPACE`, под — из env `POD_SOURCE`.
- Лидер группы в `assign(...)` собирает `userData` всех членов, группирует их в дерево `ЦОД → под → консюмер` (всё отсортировано), затем строит порядок членов **вложенным round-robin**: сначала по одному члену из каждого ЦОД, внутри ЦОД — по одному из каждого пода, внутри пода — по одному консюмеру.
- Партиции (в числовом порядке) раздаются по этому порядку. При 36 партициях и 72 членах первые 36 в порядке получают по одной партиции — и они гарантированно «размазаны» по всем ЦОД и подам.
- Результат для 4 ЦОД: по 9 активных партиций на ЦОД; ни один под не простаивает целиком, пока партиций в ЦОД не меньше, чем подов.

Основание контракта — публичный интерфейс `ConsumerPartitionAssignor` (Kafka 4.0.1):
https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/ConsumerPartitionAssignor.html

## 2. `application.yml` (самый простой способ)

Spring Kafka пробрасывает всё из `spring.kafka.consumer.properties` напрямую в Kafka-клиент, поэтому достаточно указать FQCN ассайнера:

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
    consumer:
      group-id: events-consumer-group
      properties:
        # Кастомный ассайнер. Значение — список классов; здесь один.
        partition.assignment.strategy: com.example.kafka.assignor.DatacenterAwareAssignor
        # Static membership: рестарт пода в пределах session.timeout.ms не вызывает ребаланс.
        session.timeout.ms: 60000
    listener:
      # 3 консюмера в поде = 3 отдельных члена группы (у каждого свой member.id,
      # но общие ЦОД/под из env). Меняйте под вашу топологию (3 или 6).
      concurrency: 3
```

> Примечание про `group.instance.id`: для этого ассайнера он **не обязателен** — балансировка по ЦОД/подам не зависит от порядка `member.id`, потому что мы явно передаём ЦОД/под в `userData`. Но static membership всё равно полезен: он убирает лишние ребалансы при рестарте пода. Если включаете его, задавайте уникальный `group.instance.id` на каждый консюмер (в Spring Kafka при `concurrency > 1` контейнер добавляет суффикс к базовому значению — сверьтесь с актуальной документацией Spring for Apache Kafka: https://docs.spring.io/spring-kafka/reference/).

## 3. Программная конфигурация (если нужен полный контроль над `ConsumerFactory`)

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, byte[]> consumerFactory(KafkaProperties props) {
        Map<String, Object> cfg = props.buildConsumerProperties(null);
        cfg.put(
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            com.example.kafka.assignor.DatacenterAwareAssignor.class.getName()
        );
        // При желании можно переопределить источник зоны/пода через consumer-property
        // (имеет приоритет над env — см. configure() в ассайнере):
        // cfg.put(DatacenterAwareAssignor.ZONE_OVERRIDE_CONFIG, System.getenv("DC"));
        return new DefaultKafkaConsumerFactory<>(cfg);
    }
}
```

> Сигнатура `buildConsumerProperties(...)` могла меняться между версиями Spring Boot — если компилятор ругается, используйте вариант без аргумента или актуальный из вашей версии. Класс ассайнера главное указать в `PARTITION_ASSIGNMENT_STRATEGY_CONFIG`.

## 4. OpenShift Deployment — проброс env

Namespace и имя пода отдаёт Downward API. Значения попадают в те env-переменные, которые читает ассайнер (`NAMESPACE`, `POD_SOURCE`):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: events-consumer
spec:
  replicas: 3           # 3 (или 6) подов на кластер/ЦОД
  template:
    spec:
      containers:
        - name: app
          image: registry.example.com/events-consumer:latest
          env:
            - name: NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: POD_SOURCE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "kafka-broker:9092"
```

> ⚠️ Если имя namespace **одинаковое** во всех 4 кластерах, `NAMESPACE` не различит ЦОД. Тогда:
> - добавьте отдельную переменную, различающуюся по кластеру, например `DC` (`value: "dc1"` в манифесте каждого кластера), и
> - в `DatacenterAwareAssignor` в конструкторе поменяйте строку `this.zone = ... "NAMESPACE" ...` на чтение `System.getenv("DC")` (помечено `// >>> ТОЧКА НАСТРОЙКИ ЗОНЫ`).

## 4a. Конфигурация протокола ребалансировки (EAGER / COOPERATIVE)

Протокол выбирается consumer-property `datacenter.assignor.rebalance.protocol`:

```yaml
spring:
  kafka:
    consumer:
      properties:
        partition.assignment.strategy: com.example.kafka.assignor.DatacenterAwareAssignor
        # "eager" (по умолчанию) или "cooperative"
        datacenter.assignor.rebalance.protocol: cooperative
```

Как это работает в коде:
- Значение читается в `configure(...)` и управляет двумя вещами одновременно — `supportedProtocols()` и поведением `assign(...)`.
- `eager` → `supportedProtocols()` возвращает `[EAGER]`. На ребалансе все консюмеры отзывают все партиции (stop-the-world), распределение считается заново. Просто и предсказуемо.
- `cooperative` → `supportedProtocols()` возвращает `[COOPERATIVE, EAGER]` (EAGER как фолбэк, если в группе есть члены без поддержки cooperative). В `assign(...)` включается кооперативная корректировка: партиция, которую целевым владельцем должен стать другой консюмер, но которую сейчас удерживает старый, **в этом раунде не выдаётся никому** — старый владелец её отзывает, а в следующем ребалансе она достаётся целевому. Это прямое требование контракта:

> «The assignor should not reassign any owned partitions immediately, but instead may indicate consumers the need for partition revocation so that the revoked partitions can be reassigned to other consumers in the next rebalance event.»
> https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/ConsumerPartitionAssignor.RebalanceProtocol.html

Почему один и тот же код `assign(...)` корректен в обоих режимах: при EAGER консюмеры отзывают все партиции до вступления в группу, поэтому `ownedPartitions` приходит пустым, `currentOwner` пуст, и «удержаний» не возникает — распределение выдаётся сразу и целиком. Так же устроен встроенный `CooperativeStickyAssignor`.

**Важные условия для cooperative:**
- Все члены группы должны использовать ассайнер с одинаковой конфигурацией протокола. Пока часть на `eager`, а часть на `cooperative`, группа выберет общий протокол EAGER (по фолбэку), и выигрыша от cooperative не будет.
- Правильный порядок включения — сначала выкатить `cooperative`-конфиг на все инстансы (группа продолжит работать на EAGER, т.к. это общий знаменатель), и только когда ВСЕ члены заявляют COOPERATIVE, группа переключится на него. Не включайте cooperative «частично».
- Честная оговорка: этот ассайнер распределяет по порядку (round-robin), а не «липко» (sticky). Cooperative убирает stop-the-world паузу, но при изменении состава группы партиций всё равно может переместиться много (сдвиг порядка). Минимизация числа перемещений (истинная stickiness с учётом текущих владельцев) — отдельная, более сложная доработка поверх этого кода. Если она нужна — это следующий шаг.

## 5. Развёртывание и эксплуатация

- **Общий ассайнер обязателен.** Протокол выбирает ассайнер, общий для всех членов группы. Пока часть консюмеров на старом (Range/RoundRobin), а часть на новом, группа может выбрать общий старый ассайнер, и ваша логика ЦОД не заработает. Раскатывайте новый ассайнер согласованно (в идеале — остановить группу и поднять с новым, либо контролируемый rolling с пониманием, что до полного перехода балансировка по ЦОД не гарантируется).
- **Протокол ребалансировки конфигурируется** (см. раздел 4a): `eager` по умолчанию, `cooperative` — по флагу `datacenter.assignor.rebalance.protocol`. EAGER: на ребалансе все партиции отзываются и раздаются заново (для вашей нагрузки — дёшево). COOPERATIVE: без stop-the-world, но переключать всю группу нужно согласованно.
- **НЕ включайте протокол KIP-848** (`group.protocol=consumer`, Kafka 4.0) для этой группы: там клиентские (в т.ч. кастомные) ассайнеры не поддерживаются (KAFKA-18327), назначение управляется брокером. Ваш ассайнер работает только на классическом протоколе (значение по умолчанию `group.protocol=classic`).
  Источник: https://kafka.apache.org/41/operations/consumer-rebalance-protocol/

## 6. Поведение при отказах

- **Падает целый ЦОД:** его члены исчезают из `GroupSubscription`, ассайнер пересчитывает вложенный round-robin по 3 оставшимся ЦОД — 36 партиций разложатся по 12 на ЦОД, равномерно по их подам.
- **ЦОД возвращается:** назначение считается заново (истории не хранит), система детерминированно возвращается к 9/9/9/9. Это ключевое отличие от sticky-ассайнеров, которые закрепили бы перекос.
- **Падает один под/консюмер:** его партиции уходят следующему по порядку члену; распределение по ЦОД/подам остаётся почти равномерным.

## 7. Файлы

- `DatacenterAwareAssignor.java` — сам ассайнер (положить в пакет `com.example.kafka.assignor`, поправьте под свой package).
- `DatacenterAwareAssignorTest.java` — JUnit 5 тест на равномерность (4 ЦОД × 6 подов × 3 консюмера / 36 партиций).
