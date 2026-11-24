# Аренда самокатов: команда 4

Rental выдаёт офферы и ведёт заказы в PostgreSQL. Source (самокаты, тарифные зоны, пользователи, конфигурация) и Payment — заглушки внешних сервисов. Архитектура описана в [ADR](docs/ADR.md), контракты зависимостей — в [EXTERNAL_SERVICES.md](docs/EXTERNAL_SERVICES.md), сценарии проверки — в [TEST_PLAN.md](docs/TEST_PLAN.md).

## Сервисы

| Сервис | Что делает |
| --- | --- |
| `rental api` | Клиентский API: `POST /offers`, `POST /orders`, `GET /orders/{id}`, `POST /orders/{id}/finish` |
| `rental worker` | Задачи START, SETTLE и VOID из таблицы `tasks`: захват с lease, повторы с backoff, DLQ |
| `rental maintenance` | Удаление выполненных задач, перенос заказов в `orders_history`, очистка офферов и недельных разделов; одна активная реплика по lease |
| `rental migrate` | Миграции схемы |
| `redis` | Общий кэш архивных GET для реплик Rental API |
| `rental dlq list` / `rental dlq requeue <task_id>\|--all` | Просмотр DLQ и ручной возврат задач |
| `source`, `payment` | Заглушки по контракту, данные в памяти |

Все процессы Rental — один бинарник `services/rental/cmd/rental`. У каждого процесса есть `/healthz`, `/readyz` и `/metrics`.

## Код Rental

```
services/rental/internal/
  domain/       модели, инварианты, цена и стоимость поездки, правила задач (без I/O)
  storage/      весь SQL; транзакции через Store.InTx
  rental/       сценарии API: офферы и заказы
  worker/       выполнение задач и sweeps по дедлайну и лимиту попыток
  maintenance/  архивация, очистка, разделы, метрики состояния БД
  httpapi/      HTTP-обработчики и коды ошибок
  source/       клиент Source и LRU-кэш тарифных зон
  ordercache/   кэш архивных заказов и ограничение чтений из PostgreSQL
  payment/      клиент Payment
  dynconfig/    снимок /configs: обновление раз в минуту, дефолты на холодном старте
  settings/     настройки процесса из переменных окружения
  app/          сборка процессов
```

## Команды

Нужны Go 1.26, GNU Make, C-компилятор (для `-race`) и Docker с Compose.

| Команда | Результат |
| --- | --- |
| `make` / `make check` | Юнит-тесты с `-race`, `go vet`, сборка в `build/` |
| `make db-up` | Поднять только PostgreSQL из Compose |
| `make test-integration` | Интеграционные тесты Rental на PostgreSQL (адрес берётся из `RENTAL_TEST_DATABASE_URL`, по умолчанию используется PostgreSQL из Compose) |
| `make test-functional` | Функциональные сценарии из тест-плана через HTTP на отдельной схеме PostgreSQL |
| `make up` / `make down` | Собрать и поднять / остановить весь стенд |
| `make logs` | Логи процессов Rental |

Интеграционные тесты создают собственную схему и удаляют её после себя. Без `RENTAL_TEST_DATABASE_URL` они пропускаются.

Для функциональных тестов запустите `make db-up` и `make test-functional`. Они запускают процессы Rental и тестовые зависимости на свободных локальных портах. У каждого сценария свой пакет в `services/rental/tests/`. Общие фикстуры и вспомогательный код для запуска стенда находятся в пакете `tests`. Название сценария и ссылка на соответствующий раздел [тест-плана](docs/TEST_PLAN.md) указаны в его `scenario_test.go`.

## Стенд

| Сервис | Адрес |
| --- | --- |
| Rental API (nginx перед репликами `rental-api`) | http://localhost:18080 |
| Source | http://localhost:18081 |
| Payment | http://localhost:18082 |
| Prometheus | http://localhost:19090 |
| Grafana (анонимный доступ) | http://localhost:13000 |
| PostgreSQL | `postgres://rental:rental@localhost:55432/rental` |

Число реплик задают `RENTAL_API_REPLICAS` и `RENTAL_WORKER_REPLICAS` (по умолчанию 2). Source по умолчанию содержит `scooter-1…scooter-40000` в четырёх зонах `zone-1…zone-4` и `user-1…user-40000`; объёмы меняют `SOURCE_SCOOTERS` и `SOURCE_USERS`.

Пример:

```sh
curl -X POST localhost:18080/offers -d '{"scooter_id":"scooter-5","user_id":"user-1","idempotency_id":"k1"}'
curl -X POST localhost:18080/orders -d '{"offer_id":"<offer_id>"}'
curl localhost:18080/orders/<order_id>
curl -X POST localhost:18080/orders/<order_id>/finish
```

## Оповещение о DLQ

Алерт `RentalTaskDeadLettered` срабатывает отдельно для SETTLE и VOID, если за последние две минуты были попадания в DLQ или в очереди остаются задачи этого типа. Причину можно посмотреть через `rental dlq list` и в журнале worker. [Вкладка Alerts в Prometheus](http://localhost:19090/alerts)

Чтобы проверить правило, выполните команду из корня проекта.

```sh
docker run --rm --entrypoint /bin/promtool -v "$PWD/deploy/prometheus:/rules:ro" prom/prometheus:v3.5.0 test rules /rules/tests/dlq.test.yml
```

## Настройки Rental

Обязательна только `DATABASE_URL`. Остальные переменные имеют значения по умолчанию, полный список — в [settings.go](services/rental/internal/settings/settings.go). Основные:

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `SOURCE_URL`, `PAYMENT_URL` | `http://source:8080`, `http://payment:8080` | Адреса зависимостей |
| `SOURCE_TIMEOUT`, `PAYMENT_TIMEOUT` | `500ms`, `3s` | Таймауты вызовов |
| `RENTAL_DB_MAX_CONNS` | `20` | Размер пула соединений процесса |
| `REDIS_URL` | пусто, в Compose `redis://redis:6379` | Адрес общего кэша. Без адреса работает локальный LRU |
| `ORDER_CACHE_SIZE`, `ORDER_CACHE_TTL` | `65536`, `1m` | Максимум архивных записей в LRU на реплику и базовый TTL |
| `ORDER_CACHE_REDIS_POOL_SIZE`, `ORDER_CACHE_REDIS_TIMEOUT`, `ORDER_CACHE_REDIS_COOLDOWN` | `64`, `50ms`, `5s` | Соединения Redis, таймаут операции и пауза перед пробой после сбоя |
| `ORDER_READ_DB_MAX_CONCURRENCY`, `ORDER_READ_DEGRADED_CONCURRENCY`, `ORDER_READ_DB_WAIT` | `24`, `12`, `250ms` | Лимиты параллельных GET в PostgreSQL на реплику и время ожидания слота |
| `WORKER_CONCURRENCY`, `WORKER_POLL_INTERVAL` | `64`, `100ms` | Параллелизм и опрос очереди |
| `START_MAX_ATTEMPTS`, `SETTLE_MAX_ATTEMPTS`, `VOID_MAX_ATTEMPTS` | `5`, `20`, `20` | Лимиты попыток |
| `MAINTENANCE_INTERVAL`, `MAINTENANCE_BATCH_SIZE` | `1m`, `2000` | Период и размер пачки обслуживания |

При запуске заглушки Payment с `PAYMENT_SETTLE_RESULT=failed` валидный SETTLE фиксирует долг (`DEBT`). По умолчанию результат SETTLE — `success`.
