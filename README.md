# OLTP -> OLAP benchmark

Стенд сравнивает три реализации одного процесса:

```text
CSV -> PostgreSQL -> ClickHouse
```

Поддерживаемые датасеты:

- `fact_animals`
- `med_technic`
- `turkestan_109_incidents`

Sequence UML:

- [`Python CLI`](docs/python-sequence.puml)
- [`Java CLI`](docs/java-sequence.puml)

## Что делает pipeline

Для полного CSV каждая реализация выполняет одинаковые операции:

1. Читает заголовок CSV и нормализует названия колонок.
2. Создает `oltp_source.<dataset>` в PostgreSQL.
3. Загружает CSV потоком через PostgreSQL `COPY FROM STDIN`.
4. Проверяет число строк source запросом `SELECT count(*)`.
5. Создает `<dataset>_olap` в ClickHouse.
6. Открывает один server-side cursor PostgreSQL и читает его страницами.
7. Передает страницы батчами Native-клиенту ClickHouse.
8. Клиент кодирует батч в бинарные колонночные блоки и отправляет по TCP на порт `9000`.
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
- server-side cursor и `fetchmany(fetch_size)` для чтения страниц;
- один `clickhouse-driver.Client` на весь запуск;
- Native Protocol, LZ4 и бинарные колонночные блоки без ручного TSV;
- `psutil` для CPU и RSS.

## Java CLI

Запуск:

```powershell
.\scripts\run-java.ps1 -Dataset fact_animals -File data/input/fact_animals.csv
```

Java использует:

- Spring Boot как CLI-оболочку;
- `PostgresClient`: PostgreSQL JDBC, `CopyManager` и потоковый `ResultSet`;
- `ClickHouseClient`: один Housepower Native JDBC connection и `PreparedStatement.executeBatch()`;
- `BenchmarkPipeline`: порядок стадий и сбор метрик;
- `OperatingSystemMXBean` и `/proc/self/status` для CPU и RSS.

Spring Boot только запускает CLI. ETL-код разнесен по небольшим классам, а подключения создаются один раз на запуск.

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

Обе реализации выполняют один запрос:

```sql
SELECT <columns>
FROM oltp_source.<dataset>
ORDER BY etl_row_num;
```

Python вызывает `fetchmany(fetch_size)`. Java задает `ResultSet.setFetchSize(fetch_size)` и собирает страницы того же размера. Данные идут по PostgreSQL wire protocol поверх TCP, порт `5432`.

Страница делится на батчи по `batch_size`. Python передает их в `clickhouse-driver`, Java — в Housepower Native JDBC. Оба клиента сами преобразуют строки в бинарные колонночные блоки и используют одно соединение на весь запуск.

ClickHouse принимает данные через Native Protocol поверх TCP на порту `9000`. HTTP-порт `8123` pipeline не использует; он остается доступен только для внешних административных инструментов.

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
| `serialize_ms` | `0`: сериализация скрыта внутри клиента |
| `load_ms` | бинарное кодирование клиента, TCP-передача и вставка ClickHouse |
| `verify_ms` | проверка строк ClickHouse |
| `overhead_ms` | соединения и вспомогательные операции |
| `total_ms` | полное время pipeline |
| `cpu_ms` | CPU-время процесса |
| `peak_rss_mb` | пиковая память процесса |

Посмотреть результаты:

```powershell
.\scripts\query-results.ps1
```

## Прошлый результат

Полный запуск: 9 759 911 строк, `fetch_size=50000`, `batch_size=50000`, 196 батчей.

| Реализация | Total | Строк/с | CPU | Peak RSS |
|---|---:|---:|---:|---:|
| Java | 411.9 с | 23 694 | 117.0 с | 751 MB |
| Airflow | 453.5 с | 21 524 | 146.3 с | 386 MB |
| Python | 483.3 с | 20 193 | 156.7 с | 240 MB |

Эти результаты получены старой версией с ручной TSV-сериализацией и не сравниваются напрямую с текущей версией 4:

| Стадия | Java | Airflow | Python |
|---|---:|---:|---:|
| `prepare_ms` | 193.8 с | 195.3 с | 195.3 с |
| `extract_ms` | 64.1 с | 67.5 с | 62.3 с |
| `serialize_ms` | 28.6 с | 88.0 с | 95.5 с |
| `load_ms` | 70.5 с | 62.4 с | 78.9 с |

`prepare_ms` почти одинаков, а `extract_ms` находится в близком диапазоне: на этих стадиях основную работу выполняет PostgreSQL. Java отрывается на `serialize_ms`, где `StringBuilder` и преобразование значений в UTF-8 TSV сработали примерно в 3.3 раза быстрее Python-реализации. При этом Java потребовала примерно в три раза больше памяти, поэтому ее преимущество — скорость и меньшее CPU-время, а недостаток — высокий RSS JVM.

## Текущий результат


| Реализация | Total | Строк/с | CPU | Peak RSS |
|---|---:|---:|---:|---:|
| Java | 425.7 с | 22 928 | 177.9 с | 1 028 MB |
| Python | 527.6 с | 18 498 | 179.5 с | 243 MB |
| Airflow | 621.7 с | 15 700 | 199.8 с | 361 MB |

### Влияние Native TCP и драйверов

После v4 отдельного `serialize_ms` нет: сериализация выполняется внутри драйвера и входит в `load_ms`. Поэтому сравнивается сумма `serialize_ms + load_ms` в v2 с `load_ms` в v4:

| Реализация | v1: serialize + load | v2: native load | Изменение |
|---|---:|---:|---:|
| Java | 99.1 с | 96.9 с | быстрее на 2.2% |
| Python | 174.4 с | 153.0 с | быстрее на 12.3% |
| Airflow | 150.4 с | 116.2 с | быстрее на 22.8% |

Native TCP и готовые драйверы ускорили именно путь кодирования и доставки батчей в ClickHouse. Наиболее заметен эффект у Python, поскольку исчезло построение промежуточных TSV-строк в интерпретаторе.

Полное время при этом не улучшилось: Java стала медленнее v1 примерно на 3.3%, Python на 9.2%, Airflow на 37.1%. Причина находится вне нового ClickHouse load: выросли `extract_ms`, `source_verify_ms`, `overhead_ms`, а у Airflow особенно `prepare_ms`. Следовательно, один последовательный прогон подтверждает пользу Native TCP для стадии загрузки, но не доказывает ускорение всего pipeline.

Java остаётся самой быстрой реализацией v2 и опережает Python примерно на 19% по полному времени, но использует примерно в 4.2 раза больше памяти. Для итогового вывода нужны повторные `transfer`-прогоны на одной подготовленной PostgreSQL-таблице с чередованием порядка запуска и сравнением медианы.

Удалить только историю метрик:

```powershell
.\scripts\clear-results.ps1
```

Удалить контейнеры и данные PostgreSQL/ClickHouse:

```powershell
.\scripts\clean.ps1
```

Исходные файлы из `data/input` не удаляются.
