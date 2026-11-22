# Роли сервисов и внешние контракты

| Сервис | Роль в проекте | Данные |
| --- | --- | --- |
| Rental | Основная логика: офферы, заказы, занятость самокатов, цена, фоновые задачи и очистка | PostgreSQL (`rental_db`) |
| Source | Заглушка API scooters, tariff-zones, users и configs | Данные в памяти |
| Payment | Заглушка платежного API для Rental | Состояние команд в памяти, теряется при перезапуске |

Подробно проектируем Rental. Заглушки реализуют согласованный API и позволяют проверять реакции Rental на ответы, таймауты и ошибки.

## Source

| Метод и вход | Ответ `200` |
| --- | --- |
| `GET /scooter/{id}`; `id`: строка | `id`, `zone_id`: строки; `charge`: целое число от 0 до 100 |
| `GET /tariff-zone/{id}`; `id`: строка зоны | `id`: строка; `price_per_minute`, `price_unlock`, `default_deposit`: целые неотрицательные суммы |
| `GET /user-profile/{id}`; `id`: строка пользователя | `id`: строка; `has_subscription`, `trusted`: булевы значения |
| `GET /configs`; без параметров | `price_coeff_settings: {surge, low_charge_discount}`: положительные коэффициенты; `default_retention_days`: положительное целое; `retention_days_by_zone`: словарь `zone_id → положительное целое`; `tariff_zone_cache_ttl_minutes`: положительное целое, по умолчанию 10 |

Неизвестный `id` дает `404`; некорректный запрос — `400`; недоступность источника — `5xx` или таймаут. `zone_id` из ответа scooters используется для запроса tariff-zones.

## Payment

| Метод | Запрос | Ответ `200` | Описание |
| --- | --- | --- | --- |
| `POST /payments/hold` | `operation_id`, `user_id`: строки; `amount`: положительная целая сумма; `hold_expires_at`: время UTC | `status: success` или `failed` | Удерживает сумму до `hold_expires_at`. `success` — удержание создано; `failed` — удержания нет. После VOID или при уже истекшем `hold_expires_at` первый HOLD возвращает `failed`. |
| `POST /payments/settle` | `operation_id`, `user_id`: строки; `total_amount`: целая неотрицательная сумма | `status: success` или `failed` | Принимает итоговую сумму и без HOLD; истекшее удержание не учитывает. Завершает действующее удержание и освобождает остаток. `success` — полная оплата (`PAID`); `failed` — неполная оплата (`DEBT`). |
| `POST /payments/void` | Только `operation_id`: строка | `status: success` | Отменяет удержание и запрещает поздний первый HOLD. Допустим до HOLD и после отказа HOLD. `success` возвращается и при отсутствующем или истекшем удержании. Временный сбой — `5xx` или таймаут; он не сохраняется как окончательный отказ. |
| `GET /payment/{id}` | `id`: `operation_id` | `status: HELD`, `HOLD_FAILED`, `HOLD_EXPIRED`, `PAID`, `DEBT` или `VOIDED`; до первой команды — `404` | Читает состояние платежного цикла без изменений. |

- `operation_id` идентифицирует платежный цикл; Rental передает в нем свой `order_id`.
- Первый HOLD или SETTLE связывает `operation_id` с `user_id`; другой `user_id` при следующих вызовах дает `409 USER_MISMATCH`.
- После VOID новый SETTLE, а после SETTLE новый HOLD или VOID отклоняются с `409`. Для другого действия с тем же `operation_id` Payment проверяет допустимый порядок.
- Каждое действие выполняется не более одного раза для данного `operation_id`. Повтор того же метода с теми же параметрами возвращает сохраненный окончательный результат без нового денежного эффекта: `success` или `failed` для HOLD/SETTLE, `success` для VOID; измененные параметры дают `409 PARAMETER_CONFLICT`.
- Некорректный запрос дает `400`, нарушение порядка — `409`.
