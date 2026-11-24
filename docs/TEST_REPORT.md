# Отчёт о проверке нагрузочных сценариев эмулятора

Отчёт фиксирует результаты проверки эмулятора нагрузки (`tools/loadtest`) и двух нагрузочных сценариев из [плана проверки](TEST_PLAN.md) — **базового нагрузочного прогона** и **смешанного прогона с историей** — на сервисах основной ветки `origin/master` (`a607276`).

> [!IMPORTANT]
> Прогоны выполнялись на локальном Docker Desktop (совместно с другими контейнерами на хосте) в укороченных окнах (60 секунд, 3–8 поездок/с) для валидации логики эмулятора, генерации данных, бизнес-инвариантов и первичного выявления узких мест ядра. Полномасштабная приёмка на целевых **100 заказах/с и 5 000 чтений/с за 30 минут** требует выделенного стенда без посторонней нагрузки.

---

## 1. Конфигурация стенда

| Компонент | Экземпляры | Основные настройки (`compose.yaml`) |
| --- | ---: | --- |
| `nginx` | 1 | `worker_connections 8192`, `keepalive 512`, `proxy_read_timeout 10s` |
| `rental-api` | 2 | `RENTAL_DB_MAX_CONNS=40`, `SOURCE_TIMEOUT=500ms` |
| `rental-worker` | 2 | `RENTAL_DB_MAX_CONNS=30`, `WORKER_CONCURRENCY=64`, `PAYMENT_TIMEOUT=3s` |
| `rental-maintenance` | 2 | `RENTAL_DB_MAX_CONNS=5`, `MAINTENANCE_INTERVAL=1m`, `MAINTENANCE_BATCH_SIZE=2000` |
| `postgres` | 1 | `postgres:17-alpine`, `max_connections=300`, `shared_buffers=512MB`, `TZ=UTC` |
| `source`, `payment` | по 1 | In-memory заглушки: `40 000` самокатов (4 зоны) и `40 000` пользователей |
| `prometheus` | 1 | `scrape_interval=5s` |
| `loadtest` | 1 | Отдельный контейнер (`compose.loadtest.yaml`), `-read-concurrency 1536`, `-read-queue 32768`, `-clock-skew 1µs` |

---

## 2. Проверка логики эмулятора (автотесты)

| Набор проверок | Команда | Результат |
| --- | --- | --- |
| Go статический анализ и unit/контрактные тесты с `-race` | `(cd tools/loadtest && GOWORK=off go vet ./... && GOWORK=off go test -race ./...)` | **PASS** (включая `baseline`, `mixed` со статусами `COMPLETED`/`CANCELLED`/`START_FAILED`, `bad_archive_response_fails`, `fleetPool`, `reconcile`, `readQueue`) |
| Python unit-тесты скриптов подготовки, запуска, платежей и экспорта | `python3 -m unittest -v tools/loadtest/scripts/test_scripts.py` | **PASS** (11/11, включая рендеринг 4×2 PNG через `matplotlib`) |

---

## 3. Базовый нагрузочный прогон

### Условия и шаги
- **Исходные данные:** пустая мигрированная `rental_db` (`prepare-emulator-data.py baseline`), стандартный `Source` (`40 000` самокатов / пользователей), пустой `Payment`.
- **Профиль:** `-rate 3 -warmup 10s -window 60s -min-ride 5s -max-ride 6s` (цель: 3 оффера/с, 3 старта/с, 150 `GET /orders/{id}`/с, 3 завершения/с).
- **Проверки:** `run-profile.py` (HTTP SLA + `check-profile.sql` + дренаж задач и архивации) и `check-profile-payments.py --sample-size 25`.

### Фактический результат: **PASS**

| Показатель | Цель | Факт | Статус |
| --- | --- | ---: | --- |
| Поездки (запланировано / завершено в БД) | 210 / 210 | 210 / 210 | OK |
| `offer` RPS / p95 | 3 ±5% / ≤ 1000 мс | 3.00 / 41.7 мс | OK |
| `start` RPS / p95 | 3 ±5% / ≤ 2000 мс | 3.00 / 34.3 мс | OK |
| `get_current` RPS / p95 | 150 ±5% / ≤ 300 мс | 149.10 / 17.3 мс | OK |
| `finish` RPS / p95 | 3 ±5% / ≤ 2000 мс | 3.00 / 46.4 мс | OK |
| Переход `STARTING → ACTIVE`, p95 | ≤ 2000 мс | 102.2 мс | OK |
| Ошибки HTTP / `START_FAILED` / `still_starting` / `fallback_orders` | 0 | 0 / 0 / 0 / 0 | OK |
| Покрытие комбинаций `(zone, subscription, trusted)` | 16 из 16 | 16 | OK |
| `wrong_terms` / `wrong_bills` / `wrong_retention` | 0 | 0 / 0 / 0 | OK |
| Двойные аренды (`overlapping_rentals`, `overlapping_user_rentals`, `reused_offers`) | 0 | 0 / 0 / 0 | OK |
| После дрейна: `unarchived` / `completed_tasks` / `outstanding_tasks` | 0 | 0 / 0 / 0 | OK |
| Размер строки нового заказа (`mean_order_row_bytes_in_window`) | ~1 КБ по ТЗ (факт схемы) | 176.6 Б | Зафиксировано |
| Размер БД / `orders_history` (таблицы + индексы) / разделы | — | 11.1 МиБ / 1.77 МиБ / 54 | Зафиксировано |
| Максимальное отставание архивации в окне (`archive_lag_s`) | без роста | 43.6 с (`backlog_stable=true`) | OK |
| Выборка `Payment` (`GET /payment/{id}`) | 25 / 25 `PAID` | 25 / 25 `PAID` | OK |

> [!NOTE]
> При повышении локальной нагрузки до `8 поездок/с` (400 GET/с) на том же загруженном Docker Desktop базовый прогон просел по задержкам (`get_current p95 = 1478.7 мс`, `offer p95 = 1927.4 мс`, `finish p95 = 2123.7 мс`, 3 ошибки `offer/503/SOURCE_UNAVAILABLE`), но все бизнес- и БД-инварианты по-прежнему остались нулевыми (`wrong_terms/bills/retention/overlaps = 0`, `distinct_pricing_combos = 16`).

---

## 4. Смешанный прогон с историей

### Условия и шаги
- **Исходные данные (`prepare-emulator-data.py mixed`):**
  - **53 000** актуальных архивных заказов в **53** недельных разделах (`orders_history_pYYYYMMDD`);
  - **12 000** просроченных заказов в **1** полностью истёкшем недельном разделе;
  - состав фикстуры: `62 400 COMPLETED`, `1 300 CANCELLED`, `1 300 START_FAILED`, 4 зоны, `mean_row_bytes = 1025 Б` (inline без TOAST), `invalid_rows = 0`, `ids_verified_in_postgres = true`;
  - объём истории: `74.3 МиБ` таблиц + `7.8 МиБ` индексов = **82.1 МиБ** (`shared_buffers = 512 МиБ`, `exceeds_shared_buffers = false`).
- **Профиль:** `-rate 3 -warmup 65s -window 60s -min-ride 5s -max-ride 6s -historical 15` (105 `get_current`/с + 45 `get_history`/с), флаг `--start-maintenance` (запуск `rental-maintenance` уже под живой нагрузкой для проверки параллельного `DETACH/DROP PARTITION`).

### Фактический результат: **FAIL по SLA ядра** (бизнес-инварианты и очистка — **OK**)

| Показатель | Цель | Факт | Статус |
| --- | --- | ---: | --- |
| Поездки в БД (`trips` / `finished`) | без потерь и зависаний | 374 / 374 (`scheduled=375`, 1 `offer/transport`) | Отклонение по SLA |
| `offer` RPS / p95 | 3 ±5% / ≤ 1000 мс | 3.00 / **1516.2 мс** | **FAIL** |
| `start` RPS / p95 | 3 ±5% / ≤ 2000 мс | 3.00 / 418.2 мс | OK |
| `get_current` RPS / p95 | 105 ±5% / ≤ 300 мс | 104.95 / 154.6 мс | OK |
| `get_history` RPS / p95 | 45 ±5% / ≤ 300 мс | 45.00 / **318.4 мс** | **FAIL** |
| `finish` RPS / p95 | 3 ±5% / ≤ 2000 мс | 3.00 / 1693.9 мс | OK |
| Переход `STARTING → ACTIVE`, p95 | ≤ 2000 мс | 273.6 мс | OK |
| Транспортные ошибки (таймаут 5 с во время warmup + `DETACH`) | 0 | `get_history/transport: 87`, `offer/transport: 1`, `finish/transport: 1` | **FAIL** |
| Очистка просроченного раздела под нагрузкой | `12 000 → 0`, `53 000` сохранены | `12 000 → 0`, `53 000` сохранены (`maintenance_pass=true`) | OK |
| Покрытие комбинаций `(zone, subscription, trusted)` | 16 из 16 | 16 | OK |
| `wrong_terms` / `wrong_bills` / `wrong_retention` / `fallback_orders` | 0 | 0 / 0 / 0 / 0 | OK |
| Двойные аренды / `still_starting` / `outstanding_tasks` | 0 | 0 / 0 / 0 | OK |
| После дрейна: `unarchived` / `completed_tasks` | 0 | 0 / 0 | OK |
| Наблюдаемость (`observability_complete`, `backlog_stable`) | `true` | `true` / `true` (`max_archive_lag_s = 53.5 с`) | OK |

> [!TIP]
> На более лёгком объёме истории (**10 000** retained в 53 разделах + **2 000** expired, `4 поездки/с`, `65s warmup + 60s window`, `--start-maintenance`) смешанный прогон полностью прошёл (**PASS**): `get_current p95 = 14.1 мс`, `get_history p95 = 35.6 мс`, `offer = 36.1 мс`, `start = 35.5 мс`, `finish = 76.1 мс`, `activation = 102.9 мс`, `0` ошибок, `25/25 PAID`, `export-window.py --pre-minutes 1` выгрузил полный CSV и график 4×2.

---

## 5. Итоги: что работает, а что требует доработки

### Работает стабильно
1. **Корректность ядра Rental:** отсутствие двойных аренд, точный расчёт тарифов/скидок/депозитов/стоимости (включая порог 5 секунд), точный `delete_after = closed_at + 365d`, идемпотентный `reconcile` после таймаутов, корректная работа `worker` и `Payment`.
2. **Обслуживание БД (`rental-maintenance`):** удаление `COMPLETED`-задач, перенос заказов в недельные партиции `orders_history` и физическое удаление просроченных разделов через `DETACH PARTITION CONCURRENTLY` + `DROP TABLE` без потери актуальных данных.
3. **Эмулятор и контур проверки (`tools/loadtest`):** честный open-loop, независимое распределение самокатов и пользователей, потоковая подготовка данных, полная сверка SQL/Payment/Prometheus.

### Проблемы и открытые риски (на стороне ядра и методики приёмки)
1. **Деградация `GET /orders/{id}` по 53 партициям `orders_history`:** поиск по `order_id` без `delete_after` сканирует локальные индексы всех 53 партиций. Даже при 82 МиБ истории (полностью в RAM) на локальном стенде `get_history p95` выходит за 300 мс уже на 3–8 поездках/с. Для целевых 5 000 чтений/с и 3.15 млрд записей потребуется либо кэш иммутабельных архивных заказов, либо индекс-локатор (`order_locator` / маппинг `order_id → delete_after`).
2. **Конкуренция во время `DETACH PARTITION CONCURRENTLY` под нагрузкой:** при одновременных чтениях 53 партиций и удалении просроченного раздела на 12 000 строк наблюдались таймауты `get_history` (>5 с) на локальном стенде.
3. **Отсутствие кэша перед БД (бонус за борьбу с амплификацией нагрузки на БД при падении кэша):** в ядре реализован `singleflight` в локальном LRU-кэше тарифных зон ([tariff_cache.go](../services/rental/internal/source/tariff_cache.go)), но он защищает внешний сервис `Source`, а не PostgreSQL. Кэш чтений перед БД отсутствует ([ADR.md](ADR.md#альтернативы-и-последствия)).
4. **Непроверенный целевой масштаб и дисковый I/O:** для финальной приёмки по [TEST_PLAN.md](TEST_PLAN.md) необходимо прогнать 30-минутные окна на выделенном стенде с целевым RPS (`-rate 100`, `-warmup 7m`, `-window 30m`) и объёмом истории выше `shared_buffers` (`prepare-emulator-data.py mixed --target-mib 700` или выше), а также приложить графики `export-window.py --pre-minutes 1` для подтверждения стабилизации до начала окна.
