# OLTP -> OLAP ETL benchmark

Стенд для сравнения трех реализаций переноса данных из Postgres в ClickHouse:

- `python-cli`: потоковый Python CLI;
- `java-spring-jdbc`: Java CLI на Spring Boot, загрузка CSV/XLSX в Postgres, JDBC streaming из Postgres и HTTP bulk load в ClickHouse;
- `airflow-python`: Airflow DAG, который выполняет тот же тип работ через оркестратор.

Старый сценарий "прочитать xlsx/csv в память, нормализовать и сразу положить в ClickHouse" здесь заменен на более боевой процесс:

1. CSV/XLSX потоково загружается в Postgres как подготовительный OLTP/source этап.
2. Таблица из Postgres переносится в ClickHouse как OLAP/load этап.
3. Каждая реализация пишет сопоставимые метрики в `olap_benchmark.benchmark_runs`.

## Поддерживаемые датасеты

Имена зафиксированы специально, чтобы результаты было проще сравнивать:

- `fact_animals`
- `med_technic`
- `turkestan_109_incidents`

Файл можно передать явно через `-File`, либо положить в `data/input` с именем:

```text
data/input/fact_animals.csv
data/input/fact_animals.xlsx
data/input/med_technic.csv
data/input/med_technic.xlsx
data/input/turkestan_109_incidents.csv
data/input/turkestan_109_incidents.xlsx
```

## Быстрый запуск

Из папки стенда:

```powershell
Copy-Item .env.example .env
.\scripts\start-services.ps1
```

Python, полный цикл `file -> Postgres -> ClickHouse`:

```powershell
.\scripts\run-python.ps1 -Dataset fact_animals -File data/input/fact_animals.csv
```

Быстрая проверка без полного файла:

```powershell
.\scripts\run-python.ps1 -Dataset fact_animals -File data/input/fact_animals.csv -LimitRows 1000
```

Java, полный цикл `file -> Postgres -> ClickHouse`:

```powershell
.\scripts\run-java.ps1 -Dataset fact_animals -File data/input/fact_animals.csv
```

Для полного CSV Java использует оптимизированный режим `csv-direct-copy`: исходный файл стримится прямо в Postgres `COPY` без промежуточного временного CSV. Для `XLSX` и `-LimitRows` остается безопасный режим с материализацией очищенного CSV.

Java ClickHouse writer можно переключать:

```powershell
.\scripts\run-java.ps1 -Dataset fact_animals -File data/input/fact_animals.csv -ClickHouseWriter http-tsv
.\scripts\run-java.ps1 -Dataset fact_animals -File data/input/fact_animals.csv -ClickHouseWriter client-v3
```

`http-tsv` - исходный ручной HTTP writer, он остается режимом по умолчанию. `client-v3` использует официальный ClickHouse Java `client-v2` и его `insert(..., InputStream, ClickHouseFormat.TSV, InsertSettings)`.

Airflow:

```powershell
.\scripts\run-airflow.ps1 -Dataset fact_animals -File data/input/fact_animals.csv
```

Если Airflow уже был запущен до добавления DAG или Python-пакета, пересоберите его:

```powershell
docker compose up -d --build airflow
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags list-import-errors
```

Airflow UI:

```text
http://localhost:8080
admin / admin
```

Посмотреть результаты:

```powershell
.\scripts\query-results.ps1
```

## Что измеряется

Основная таблица метрик: `olap_benchmark.benchmark_runs`.

Ключевые поля:

- `prepare_ms`: потоковая загрузка CSV/XLSX в Postgres;
- `extract_ms`: время чтения из Postgres;
- `load_ms`: время вставки в ClickHouse;
- `verify_ms`: проверка количества строк в target;
- `total_ms`: полный измеренный интервал;
- `rows_per_sec`, `mb_per_sec`: пропускная способность;
- `fetch_size`, `batch_size`, `batch_count`: параметры и фактическая форма батчей;
- `logical_bytes`: оценочный объем данных по строковым значениям;
- `peak_rss_mb`: пик памяти процесса;
- `cpu_ms`: CPU-время процесса или основного Java thread;
- `extra_json`: число колонок, список колонок и технические заметки.

Агрегированный отчет:

```powershell
docker compose exec clickhouse clickhouse-client --queries-file /docker-entrypoint-initdb.d/benchmark_report.sql
```

## Эксперименты

Для честного сравнения именно `Postgres -> ClickHouse` сначала подготовьте source один раз, затем запускайте реализации с `-SkipPrepare`:

```powershell
.\scripts\prepare-source.ps1 -Dataset fact_animals -File data/input/fact_animals.xlsx
.\scripts\run-python.ps1 -Dataset fact_animals -SkipPrepare -FetchSize 10000 -BatchSize 10000
.\scripts\run-java.ps1 -Dataset fact_animals -SkipPrepare -FetchSize 10000 -BatchSize 10000
.\scripts\run-airflow.ps1 -Dataset fact_animals -SkipPrepare -FetchSize 10000 -BatchSize 10000
```

Или одной командой:

```powershell
.\scripts\run-all.ps1 -Dataset fact_animals -File data/input/fact_animals.xlsx -FetchSize 100000 -BatchSize 50000
```

Для оценки полного процесса запускайте Python, Java и Airflow без `-SkipPrepare`; тогда `prepare_ms` войдет в метрики всех трех реализаций.

```powershell
.\scripts\run-python.ps1 -Dataset fact_animals -FetchSize 100000 -BatchSize 50000
.\scripts\run-java.ps1 -Dataset fact_animals -FetchSize 100000 -BatchSize 50000
.\scripts\run-airflow.ps1 -Dataset fact_animals -FetchSize 100000 -BatchSize 50000
```

Практически полезные сравнения:

- при одинаковом `rows` сравнить `extract_ms` и `load_ms`;
- если `load_ms` доминирует, смотреть `batch_size`, `batch_count`, `mb_per_sec`;
- если `extract_ms` доминирует, смотреть `fetch_size` и CPU;
- если Airflow сильно медленнее на малых данных, отдельно учитывать orchestration overhead;
- если Java быстрее/медленнее Python, смотреть `cpu_ms`, `peak_rss_mb`, `batch_count` и фактический `mb_per_sec`.

## Архитектурная идея

Postgres source-таблица создается динамически в схеме `oltp_source`. Все исходные колонки приводятся к `TEXT`, потому что задача стенда - измерять extract/load, а не качество доменной типизации. ClickHouse target создается как `<dataset>_olap` в базе `olap_benchmark`, с `MergeTree ORDER BY etl_row_num`.

Внутри контейнеров рабочая директория называется `/benchmark`. Локальная папка `data` монтируется как `/benchmark/data`, поэтому путь `data/input/fact_animals.xlsx` в скриптах соответствует контейнерному пути `/benchmark/data/input/fact_animals.xlsx`.

Такой подход позволяет запускать один и тот же стенд на разных исходных файлах без ручного DDL для каждого XLSX/CSV и не загружать весь файл в оперативную память.
