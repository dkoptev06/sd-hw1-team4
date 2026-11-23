# Сервис аренды самокатов

Rental выдаёт офферы и ведёт заказы в PostgreSQL 17. Source предоставляет данные самокатов, пользователей, тарифных зон и конфигурации. Payment обрабатывает платёжные операции. У каждого сервиса свой Go-модуль и Dockerfile. Корневой `go.work` объединяет модули для локальной разработки.

## Проверки и сборка

Для команд нужны Go 1.26 и GNU Make. Для запуска Go-тестов с детектором гонок нужен C-компилятор.

| Команда | Результат |
| --- | --- |
| `make check` | Тесты, `go vet` и сборка. Эту же цель запускает `make` без аргументов. |
| `make test` | Тесты всех трёх модулей с детектором гонок. Проверки, которым нужна БД, пропускаются. |
| `make test-integration` | Тесты Rental с детектором гонок и подключением к PostgreSQL. |
| `make vet` | Проверка всех модулей через `go vet`. |
| `make build` | Бинарники `rental-api`, `rental-worker`, `rental-maintenance`, `source` и `payment` в каталоге `build/`. |
| `make up` | Проверка Compose, сборка образов и запуск локального стенда. |

Для интеграционных тестов нужен PostgreSQL 17. Тесты создают и удаляют собственные схемы. Пример для локальной БД из Compose:

```sh
docker compose up -d postgres
RENTAL_TEST_DATABASE_URL='postgres://rental:rental@127.0.0.1:55432/rental?sslmode=disable' make test-integration
```

Дождитесь готовности PostgreSQL перед запуском тестов. При изменении `RENTAL_POSTGRES_PORT` укажите тот же порт в строке подключения.

## Локальный стенд

Установите Docker с Compose и выполните `make up`. Compose применяет миграции Rental перед запуском API.

| Сервис | Адрес по умолчанию |
| --- | --- |
| Rental API | `http://localhost:18080` |
| Source | `http://localhost:18081` |
| Payment | `http://localhost:18082` |

У каждого HTTP-сервиса есть `/healthz` и `/metrics`, у Rental API также есть `/readyz`. Сейчас предметные HTTP-маршруты отвечают `501 NOT_IMPLEMENTED`. Worker и maintenance собираются как отдельные бинарники Rental. Их циклы будут подключены в соответствующих срезах.

## Документация проекта

- [Архитектурное решение](docs/ADR.md)
- [Контракты внешних сервисов](docs/EXTERNAL_SERVICES.md)
- [План проверок](docs/TEST_PLAN.md)
- [Распределение работы](docs/WORK_SPLIT.md)
- [Бюджет ресурсов](docs/RESOURCE_BUDGET.md)
- [Исходная заготовка задания](source-task/readme.MD)
