# OLTP -> OLAP benchmark

Стенд сравнивает три реализации одного процесса:

```text
CSV -> PostgreSQL -> ClickHouse
```

Поддерживаемые датасеты:

- `fact_animals`
- `med_technic`
- `turkestan_109_incidents`

## Что делает pipeline

Для полного CSV каждая реализация выполняет одинаковые операции:

1. Читает заголовок CSV и нормализует названия колонок.
2. Создает `oltp_source.<dataset>` в PostgreSQL.
3. Загружает CSV потоком через PostgreSQL `COPY FROM STDIN`.
4. Проверяет число строк source запросом `SELECT count(*)`.
5. Создает `<dataset>_olap` в ClickHouse.
6. Читает PostgreSQL страницами по `etl_row_num`.
7. Преобразует строки в ClickHouse `TabSeparated`.
8. Отправляет батчи в ClickHouse через HTTP POST.
9. Проверяет число строк target запросом `SELECT count()`.
10. Записывает метрики в `olap_benchmark.benchmark_runs`.

Source-колонки создаются как `TEXT`. В ClickHouse они создаются как `Nullable(String)`. Техническая колонка `etl_row_num` используется для последовательного чтения.

## Python CLI

Запуск:

```powershell
.\scripts\run-python.ps1 -Dataset fact_animals -File data/input/fact_animals.csv
```

Python использует:

- `psycopg` для PostgreSQL и `COPY`;
- keyset-запросы для чтения страниц;
- стандартный `urllib.request` для HTTP POST в ClickHouse;
- `psutil` для CPU и RSS.

## Java CLI

Запуск:

```powershell
.\scripts\run-java.ps1 -Dataset fact_animals -File data/input/fact_animals.csv
```

Java использует:

- Spring Boot как CLI-оболочку;
- PostgreSQL JDBC и `CopyManager` для `COPY`;
- JDBC для чтения страниц;
- Java `HttpClient` для HTTP POST в ClickHouse;
- `OperatingSystemMXBean` и `/proc/self/status` для CPU и RSS.

Spring Boot не выполняет ETL. Перенос реализован непосредственно через JDBC, `CopyManager` и `HttpClient`.

## Airflow

Запуск:

```powershell
.\scripts\run-airflow.ps1 -Dataset fact_animals -File data/input/fact_animals.csv
```

DAG `oltp_olap_benchmark` состоит из двух задач:

```text
prepare -> transfer
```

Airflow вызывает те же Python-функции, что Python CLI. Его отличие заключается в оркестрации: регистрация DAG, запуск задач, хранение состояния и ожидание завершения.

Скрипт ждет фактического состояния `success`. Airflow UI: `http://localhost:8080`, `admin / admin`.

## Чтение PostgreSQL

Страницы читаются запросом:

```sql
SELECT <columns>
FROM oltp_source.<dataset>
WHERE etl_row_num > :last_id
ORDER BY etl_row_num
LIMIT :fetch_size;
```

`fetch_size` задает число строк одной PostgreSQL-страницы.

Страница делится на батчи по `batch_size`. Каждый батч отправляется отдельным HTTP INSERT:

```sql
INSERT INTO <dataset>_olap (...)
FORMAT TabSeparated
```

## Режимы запуска

Полный процесс для всех реализаций:

```powershell
.\scripts\run-all.ps1 -Mode Full -Dataset fact_animals -File data/input/fact_animals.csv -FetchSize 50000 -BatchSize 50000
```

Только PostgreSQL -> ClickHouse на общей source-таблице:

```powershell
.\scripts\run-all.ps1 -Mode Transfer -Dataset fact_animals -File data/input/fact_animals.csv -FetchSize 50000 -BatchSize 50000
```

Быстрая проверка:

```powershell
.\scripts\run-all.ps1 -Mode Full -Dataset fact_animals -File data/input/fact_animals.csv -LimitRows 1000 -FetchSize 500 -BatchSize 500
```

Для сравнения производительности используйте полный CSV без `-LimitRows`. XLSX и `-LimitRows` предназначены для функциональной проверки.

## Метрики

| Метрика | Стадия |
|---|---|
| `prepare_ms` | CSV -> PostgreSQL COPY |
| `source_verify_ms` | проверка строк PostgreSQL |
| `target_setup_ms` | создание target ClickHouse |
| `extract_ms` | чтение страниц PostgreSQL |
| `serialize_ms` | формирование UTF-8 TSV |
| `load_ms` | HTTP POST в ClickHouse |
| `verify_ms` | проверка строк ClickHouse |
| `overhead_ms` | соединения и вспомогательные операции |
| `total_ms` | полное время pipeline |
| `cpu_ms` | CPU-время процесса |
| `peak_rss_mb` | пиковая память процесса |

Посмотреть результаты:

```powershell
.\scripts\query-results.ps1
```

## Результат fact_animals

Полный запуск: 9 759 911 строк, `fetch_size=50000`, `batch_size=50000`, 196 батчей.

| Реализация | Total | Строк/с | CPU | Peak RSS |
|---|---:|---:|---:|---:|
| Java | 411.9 с | 23 694 | 117.0 с | 751 MB |
| Airflow | 453.5 с | 21 524 | 146.3 с | 386 MB |
| Python | 483.3 с | 20 193 | 156.7 с | 240 MB |

Java быстрее Python примерно на 15%. Основная причина отрыва — сериализация строк в TSV:

| Стадия | Java | Airflow | Python |
|---|---:|---:|---:|
| `prepare_ms` | 193.8 с | 195.3 с | 195.3 с |
| `extract_ms` | 64.1 с | 67.5 с | 62.3 с |
| `serialize_ms` | 28.6 с | 88.0 с | 95.5 с |
| `load_ms` | 70.5 с | 62.4 с | 78.9 с |

`prepare_ms` почти одинаков, а `extract_ms` находится в близком диапазоне: на этих стадиях основную работу выполняет PostgreSQL. Java отрывается на `serialize_ms`, где `StringBuilder` и преобразование значений в UTF-8 TSV сработали примерно в 3.3 раза быстрее Python-реализации. При этом Java потребовала примерно в три раза больше памяти, поэтому ее преимущество — скорость и меньшее CPU-время, а недостаток — высокий RSS JVM.

Это результаты одного последовательного прогона. Для итогового вывода следует выполнить несколько повторений, поскольку порядок запуска и прогретые кэши могут влиять на `source_verify_ms` и `load_ms`.

Удалить только историю метрик:

```powershell
.\scripts\clear-results.ps1
```

Удалить контейнеры и данные PostgreSQL/ClickHouse:

```powershell
.\scripts\clean.ps1
```

Исходные файлы из `data/input` не удаляются.
