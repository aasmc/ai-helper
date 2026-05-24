Основной гайд по миграции на SB4 https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide

Версия 4.0.6
Spring Cloud версия 2025.1.1

Стоит прогнать тулзу по проверке пропертей - она поможет выявить проперти, которые были удалены / переименованы: https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide#configuration-properties-migration

Теперь используется модульный подход к зависимостям - https://spring.io/blog/2025/10/28/modularizing-spring-boot. Придется явно подключать новые стартеры и их тестовые версии, чтобы получить автоконфигурации. https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide#module-dependencies. Например, для использования RestTemplate или RestClient потребуется подключить стартер spring-boot-starter-restclient и его тестовую версию spring-boot-starter-restclient-test. Для стартера spring-boot-starter-webmvc также потребуется тестовая версия spring-boot-starter-webmvc-test и т.п. 

Важно! Некоторые стартеры были помечены как Deprecated - https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide#deprecated-starters

В SB 4 принято использовать JSpecify аннотации. У нас много где в проекте используются другие - потребуется замена. https://jspecify.dev/docs/user-guide/

EnvironmentPostProcessor переехал в другой пакет. Старая версия пока еще доступна, но потребуется поменять в стартерах. https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide#bootstrapregistry-and-environmentpostprocessor-package-changes

Зависимости, которые помечены в Maven как optional более не попадают в uber jar, надо проверить, что без них в рантайме нет проблем. https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide#optional-dependencies-in-maven

HttpMessageConverter помечен как Deprecated. **If you are contributing HttpMessageConverter beans to the context (like a JacksonJsonHttpMessageConverter), this is not supported anymore and you will need to update your configuration.** https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide#httpmessageconverters-deprecation Вместо этого предлагается подход с кастомайзерами https://docs.spring.io/spring-boot/4.0-SNAPSHOT/reference/web/servlet.html#web.servlet.spring-mvc.message-converters

В Spring Boot 4 более нет поддержки версионирования Spring Retry https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide#dependency-management-for-spring-retry. Основная функциональность Spring Retry добавлена в Core модуль https://spring.io/blog/2025/09/09/core-spring-resilience-features. Если у нас в стартере Config Server настроены ретраи для подключения к CS, то требуется явная зависимость на Spring Retry. 

Spring Kafka также перешли на использование новых ретраев: https://github.com/spring-projects/spring-kafka/pull/4059. Возможно, потребуется учесть при настройке консюмеров. 

Мажорное обновление Jackson до 3 версии.
Тут много изменений:
- поменялись пакеты
- поменялись сущности ObjectMapper - общая сущность, для работы с JSON нужен JsonMapper
- все исключения от RuntimeException. JsonProcessingException больше нет - есть JacksonException. 
- JsonMappingException стало DatabindException
 
Jackson 3 - https://spring.io/blog/2025/10/07/introducing-jackson-3-support-in-spring
Гайд по миграции от FasterXML https://github.com/FasterXML/jackson/blob/main/jackson3/MIGRATING_TO_JACKSON_3.md

Изменения дефолтных настроек: https://github.com/FasterXML/jackson-future-ideas/wiki/JSTEP-2

Из важного SORT_PROPERTIES_ALPHABETICALLY: default to true - если в тестах есть завязка на порядок, то могут покраснеть. 

In Jackson 3.x, DeserializationFeature.FAIL_ON_TRAILING_TOKENS is enabled by default (it was off by default in 2.x). This adds a small amount of overhead because the parser validates there is no extra content after a successful value parse.

Советы по миграции Jackson 2 -> Jackson 3 от OpenRewrite https://docs.openrewrite.org/recipes/java/jackson/upgradejackson_2_3

Spring Data. Release notes https://github.com/spring-projects/spring-data-commons/wiki/Spring-Data-2025.1-Release-Notes

Из явного, что нам надо учесть - удалено исключение DbActionExecutionException - потребуется переработать обработчики ошибок. https://github.com/spring-projects/spring-data-commons/wiki/Spring-Data-2025.1-Release-Notes#%EF%B8%8F-breaking-change-removal-of-dbactionexecutionexception

Spring 7. Release Notes https://github.com/spring-projects/spring-framework/wiki/Spring-Framework-7.0-Release-Notes

Удален API HttpComponentsClientHttpRequestFactory#setConnectTimeout. Было issue на тему некорректного использования ConnectionTimeout вместо ReadTimeout при переиспользовании соединения, если было SSL. https://github.com/spring-projects/spring-framework/issues/35748 

Deprecations https://github.com/spring-projects/spring-framework/wiki/Spring-Framework-7.0-Release-Notes#deprecations 
RestTemplate помечен как Deprecated - надо запланировать переход на RestClient. 

Важно! CORS запросы больше не отклоняются, даже если конфигурация CORS отсутствует https://github.com/spring-projects/spring-framework/issues/31839