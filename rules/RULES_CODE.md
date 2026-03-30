# RULES_CODE.md — Правила написания кода

Ты — AI-агент, пишущий production-код на Java и Spring Boot 3.
Стек: Java 17+, Spring Boot 3.x, Spring Data JDBC / Spring Data JPA, PostgreSQL, Apache Kafka, Resilience4j, OpenShift, Helm.

Соблюдай каждое правило ниже. Если правило противоречит запросу пользователя, сообщи о противоречии и запроси уточнение.

---

## 1. Версии и совместимость

1.1. Используй Java 17 или выше. Не используй API, удалённые в Java 17+ (например, `javax.*` заменён на `jakarta.*` в Spring Boot 3).

1.2. Все импорты пакетов `javax.servlet`, `javax.persistence`, `javax.validation` и прочих `javax.*` замени на `jakarta.*`. Исключение — `javax.crypto`, `javax.net`, `javax.sql`, которые остались в JDK.

1.3. Используй Spring Boot 3.3+ BOM для управления версиями зависимостей. Не указывай версии транзитивных зависимостей вручную, если они управляются BOM.

1.4. Для Kafka используй `spring-kafka`, совместимый с Spring Boot 3.x.

1.5. Для Resilience4j используй `resilience4j-spring-boot3` (не `resilience4j-spring-boot2`).

---

## 2. Структура проекта

2.1. Следуй пакетной структуре по доменам (package-by-feature), а не по слоям (package-by-layer).
Правильно:
```
com.company.project.order.controller
com.company.project.order.service
com.company.project.order.repository
com.company.project.order.model
```
Неправильно:
```
com.company.project.controllers
com.company.project.services
com.company.project.repositories
```

2.2. Каждый класс выполняет одну ответственность. Контроллер не содержит бизнес-логику. Сервис не выполняет маппинг DTO в Entity напрямую — используй отдельный маппер.

2.3. Не размещай конфигурационные классы (`@Configuration`) внутри доменных пакетов. Храни их в отдельном пакете `config` или `infrastructure.config`.

---

## 3. Spring Boot конфигурация

3.1. Используй `application.yml` как основной формат конфигурации. Не используй `application.properties`.

3.2. Для типобезопасной конфигурации используй `@ConfigurationProperties` с аннотацией `@Validated`. Не читай значения через `@Value`, кроме случаев, когда нужно внедрить одно единственное значение.

3.3. Не храни секреты (пароли, токены, ключи) в `application.yml`. Используй переменные окружения или Kubernetes Secrets, подставляемые через `${ENV_VAR}`.

3.4. Профили Spring (`spring.profiles.active`) используй только для тестов. Не используй профили для переключения бизнес-логики.

---

## 4. REST API

4.1. Контроллеры аннотируй `@RestController`. Не используй `@Controller` + `@ResponseBody` по отдельности.

4.2. Используй корректные HTTP-методы: `GET` для чтения, `POST` для создания, `PUT` для полной замены, `PATCH` для частичного обновления, `DELETE` для удаления.

4.3. Возвращай корректные HTTP-статусы: `201 Created` при создании, `204 No Content` при удалении без тела ответа, `404 Not Found` если ресурс не найден, `400 Bad Request` при нарушении бизнес-правил.

4.4. Для обработки ошибок используй `@RestControllerAdvice` с методами `@ExceptionHandler`. Не обрабатывай исключения внутри каждого контроллера отдельно.

4.5. Формат тела ошибки — единообразная структура:
```json
{
  "code": "Bad Request",
  "message": "Описание ошибки"
}
```

4.6. Все эндпоинты API версионируй через путь: `/api/v1/...`. Не используй версионирование через заголовки.

4.7. Для валидации входных данных используй `jakarta.validation` аннотации (`@NotNull`, `@Size`, `@Valid`) на DTO. Не валидируй вручную в контроллере или сервисе то, что покрывается декларативной валидацией.

---

## 5. Слой данных — Spring Data JPA

5.1. Entity-классы аннотируй `@Entity`. Используй `@Table(name = "...")` для явного указания имени таблицы.

5.2. Для первичных ключей используй `@Id` + `@GeneratedValue(strategy = GenerationType.IDENTITY)` при использовании PostgreSQL `SERIAL`/`BIGSERIAL`, либо `GenerationType.SEQUENCE` с явно указанной `@SequenceGenerator` для PostgreSQL sequence.

5.3. Не используй `GenerationType.AUTO` — поведение зависит от провайдера и СУБД и непредсказуемо.

5.4. Все связи `@OneToMany`, `@ManyToOne`, `@ManyToMany` должны иметь явно указанный `fetch = FetchType.LAZY`. Не полагайся на значения по умолчанию.

5.5. Не используй `CascadeType.ALL` и `CascadeType.REMOVE` без явного обоснования. Каскадное удаление в JPA может привести к массовому удалению данных и N+1 DELETE-запросам. Если каскад нужен — используй каскад на уровне БД (`ON DELETE CASCADE`).

5.6. Для запросов, возвращающих списки, используй пагинацию через `Pageable` и возвращай `Page<T>` или `Slice<T>`. Не возвращай `List<T>` из репозитория для таблиц с потенциально большим числом записей.

5.7. Не вызывай `entityManager.flush()` и `entityManager.clear()` без явной необходимости и комментария, зачем это сделано.

---

## 6. Слой данных — Spring Data JDBC

6.1. Если используется Spring Data JDBC, не применяй JPA-аннотации (`@Entity`, `@Column`, `@Table` из `jakarta.persistence`). Используй `@Table` и `@Column` из `org.springframework.data.relational.core.mapping`.

6.2. В Spring Data JDBC агрегат — это корневая сущность с вложенными объектами. Не создавай репозиториев для дочерних сущностей агрегата — управляй ими через корневой репозиторий.

6.3. Для кастомных запросов используй `@Query` с нативным SQL. Spring Data JDBC не поддерживает JPQL.

---

## 7. PostgreSQL

7.1. Миграции БД управляй через Liquibase. Не используй `spring.jpa.hibernate.ddl-auto=update` или `create` ни в каком окружении, кроме локального для быстрого прототипирования.

7.2. Имена таблиц и столбцов в миграциях пиши в `snake_case`. Не используй `camelCase` и кавычки в именах объектов БД.

7.3. Каждая миграция идемпотентна или версионирована. Не изменяй уже применённые миграции.

7.4. Индексы создавай для всех столбцов, участвующих в `WHERE`, `JOIN`, `ORDER BY` в частых запросах. Добавляй индексы в миграции, а не полагайся на автоматическое создание. Если требуется, сделай составной или частичный индекс.

7.5. Используй типы PostgreSQL адекватно: `TIMESTAMPTZ` для хранения дат с временной зоной, `UUID` для идентификаторов, `JSONB` для слабоструктурированных данных. Не храни даты как строки.

---

## 8. Apache Kafka

8.1. Для продюсера используй `KafkaTemplate`. Не создавай `KafkaProducer` вручную.

8.2. Для консюмера используй `@KafkaListener`. Указывай `groupId` явно в аннотации или конфигурации.

8.3. Сообщения сериализуй/десериализуй в JSON через `JsonSerializer` / `JsonDeserializer` из `spring-kafka`. Укажи `spring.kafka.consumer.properties.spring.json.use.type.headers=false` для консюмера и `spring.kafka.consumer.properties.spring.json.add.type.headers=false` для продюсера.

8.4. Обработку ошибок консюмера настраивай через `DefaultErrorHandler` с `FixedBackOff` или `ExponentialBackOff`. Не игнорируй ошибки десериализации — настрой `ErrorHandlingDeserializer`.

8.5. Каждый Kafka-консюмер должен быть идемпотентным. Если обработка сообщения включает запись в БД, используй уникальный идентификатор сообщения (например, заголовок `message-id` или поле в payload) для дедупликации.

8.6. Не выполняй длительные синхронные операции (HTTP-вызовы, тяжёлые SQL-запросы) внутри `@KafkaListener` без вынесения в отдельный поток или использования `CompletableFuture`. Блокировка потока консюмера приводит к ребалансировке consumer group.

8.7. Имена топиков задавай через конфигурацию (`application.yml`), а не хардкодь в аннотациях.

---

## 9. Resilience4j

9.1. Для аннотаций `@CircuitBreaker`, `@Retry`, `@RateLimiter`, `@Bulkhead`, `@TimeLimiter` указывай параметр `name`, совпадающий с именем инстанса в конфигурации.

9.2. Параметры Circuit Breaker, Retry и прочих паттернов задавай в `application.yml`, а не в коде. Код должен содержать только аннотации с `name` и опционально `fallbackMethod`.

9.3. Fallback-метод должен иметь ту же сигнатуру, что и основной метод, плюс параметр `Throwable` (или конкретный тип исключения) последним аргументом.

9.4. Порядок применения аннотаций Resilience4j имеет значение. Стандартный порядок: `Retry( CircuitBreaker( RateLimiter( TimeLimiter( Bulkhead( method ) ) ) ) )`. Если нужен другой порядок — настрой через `resilience4j.?.{name}.order` в конфигурации.

9.5. Не оборачивай в `@Retry` методы, которые не идемпотентны. Повтор неидемпотентной операции (например, POST-запрос, создающий запись без проверки дубликата) приведёт к побочным эффектам.

---

## 10. Обработка ошибок и логирование

10.1. Используй SLF4J (`org.slf4j.Logger`) через `LoggerFactory.getLogger(ClassName.class)` или Lombok аннотацию `@Slf4j`. Не используй `System.out.println`, `System.err.println`, `e.printStackTrace()`.

10.2. В лог-сообщениях используй параметризованные строки: `log.info("Order {} created for user {}", orderId, userId)`. Не конкатенируй строки: `log.info("Order " + orderId + " created")`.

10.3. При логировании исключений передавай объект исключения последним аргументом: `log.error("Failed to process order {}", orderId, exception)`. Не вызывай `exception.getMessage()` вручную.

10.4. Не ловай `Exception` или `Throwable` без явной необходимости. Лови конкретные типы исключений.

10.5. Для бизнес-ошибок создавай собственные классы исключений, наследуемые от `RuntimeException`. Не используй `RuntimeException` напрямую с текстовым сообщением.

10.6. Не подавляй исключения в пустом `catch`-блоке. Если исключение намеренно игнорируется — добавь комментарий с объяснением.

---

## 11. Безопасность

11.1. Не логируй персональные данные (PII): пароли, номера документов, токены, ключи. Если нужно логировать идентификатор — маскируй его.

11.2. SQL-запросы выполняй только через параметризованные запросы (prepared statements, Spring Data `@Query` с `:param`). Не конкатенируй пользовательский ввод в строку SQL.

11.3. Не отдавай клиенту stack trace в теле ответа на ошибку. В `@RestControllerAdvice` формируй безопасное сообщение без деталей реализации.

---

## 12. Стиль кода

12.1. Имена классов — `PascalCase`. Имена методов и переменных — `camelCase`. Константы — `UPPER_SNAKE_CASE`.

12.2. Не используй `var` для типов, где результирующий тип неочевиден при чтении кода. `var list = getOrders()` допустим, если метод возвращает `List<Order>` и это ясно из контекста. `var result = process(data)` — недопустим, если тип `result` неочевиден.

12.3. Используй `record` для DTO (Data Transfer Objects) и Value Objects, если они неизменяемы. Не создавай POJO с геттерами, сеттерами и конструкторами для DTO, если `record` подходит.

12.4. Не используй Lombok в новом коде, если применимы Java records. Если Lombok используется — допустимы только `@Data`, `@Value`, `@Builder`, `@Slf4j`, `@RequiredArgsConstructor`. Не используй `@SneakyThrows`, `@UtilityClass`, `@Getter(lazy=true)`.

12.5. Внедрение зависимостей — только через конструктор. Не используй `@Autowired` на полях. Если в классе один конструктор, аннотация `@Autowired` на нём не нужна.

12.6. Не возвращай `null` из методов сервисного слоя. Используй `Optional<T>` для случаев, когда результат может отсутствовать, или выбрасывай исключение, если отсутствие результата — ошибка.

12.7. Не создавай утилитных классов с `static`-методами для бизнес-логики. `static`-методы допустимы только для чистых функций без побочных эффектов (конвертеры, форматтеры).

---

## 13. OpenShift и Helm

13.1. Приложение должно корректно обрабатывать сигнал `SIGTERM` — graceful shutdown. Настрой `server.shutdown=graceful` и `spring.lifecycle.timeout-per-shutdown-phase` в `application.yml`.

13.2. Реализуй endpoint-ы для liveness (`/actuator/health/liveness`) и readiness (`/actuator/health/readiness`) проб. Используй Spring Boot Actuator с включёнными Kubernetes probes: `management.endpoint.health.probes.enabled=true`.

13.3. В Helm-чартах не хардкодь значения ресурсов (CPU, memory), реплик, имён образов. Всё параметризуй через `values.yaml`.

13.4. Логи пиши в stdout в формате JSON. Используй Logback + `logstash-logback-encoder` для структурированного логирования.

13.5. Не сохраняй состояние в файловой системе контейнера. Приложение должно быть stateless. Состояние храни в PostgreSQL, кеше (Redis) или Kafka.
