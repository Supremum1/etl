package bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class App implements CommandLineRunner {
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        Args parsed = Args.parse(args);
        Metrics metrics = transfer(parsed);
        System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(metrics.toMap()));
    }

    private Metrics transfer(Args args) throws Exception {
        String dataset = Identifiers.requireDataset(args.dataset);
        String sourceTable = dataset;
        String targetTable = dataset + "_olap";
        int fetchSize = args.fetchSize;
        int batchSize = args.batchSize;

        String pgUrl = "jdbc:postgresql://" + env("POSTGRES_HOST", "localhost") + ":"
            + env("POSTGRES_PORT", "5432") + "/" + env("POSTGRES_DB", "etl_source");
        String pgUser = env("POSTGRES_USER", "etl");
        String pgPassword = env("POSTGRES_PASSWORD", "etl");
        ClickHouseHttp ch = new ClickHouseHttp(
            env("CLICKHOUSE_HOST", "localhost"),
            Integer.parseInt(env("CLICKHOUSE_PORT", "8123")),
            env("CLICKHOUSE_DATABASE", "olap_benchmark"),
            env("CLICKHOUSE_USER", "default"),
            env("CLICKHOUSE_PASSWORD", "etl")
        );
        ClickHouseClientV3 chClient = "client-v3".equals(args.clickHouseWriter)
            ? new ClickHouseClientV3(
                env("CLICKHOUSE_HOST", "localhost"),
                Integer.parseInt(env("CLICKHOUSE_PORT", "8123")),
                env("CLICKHOUSE_DATABASE", "olap_benchmark"),
                env("CLICKHOUSE_USER", "default"),
                env("CLICKHOUSE_PASSWORD", "etl"))
            : null;

        long started = System.nanoTime();
        long cpuStarted = currentThreadCpuMs();
        Metrics metrics = new Metrics();
        metrics.implementation = "java-spring-jdbc";
        metrics.dataset = dataset;
        metrics.sourceTable = "oltp_source." + sourceTable;
        metrics.targetTable = ch.database + "." + targetTable;
        metrics.fetchSize = fetchSize;
        metrics.batchSize = batchSize;

        try (Connection pg = DriverManager.getConnection(pgUrl, pgUser, pgPassword)) {
            if (!args.skipPrepare) {
                prepareSource(pg, args, metrics);
            }
            pg.setAutoCommit(false);
            List<String> columns = pgColumns(pg, sourceTable);
            createTarget(ch, targetTable, columns, args.truncateTarget);

            String sql = "SELECT " + joinQuotedPg(columns) + " FROM oltp_source."
                + quotePg(sourceTable) + " ORDER BY etl_row_num";
            long extractStart = System.nanoTime();
            try (PreparedStatement select = pg.prepareStatement(
                    sql,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY)) {
                select.setFetchSize(fetchSize);
                try (ResultSet rs = select.executeQuery()) {
                    metrics.extractMs += elapsedMs(extractStart);
                    List<List<Object>> batch = new ArrayList<>(batchSize);
                    while (true) {
                        long rowStart = System.nanoTime();
                        boolean hasRow = rs.next();
                        metrics.extractMs += elapsedMs(rowStart);
                        if (!hasRow) {
                            break;
                        }
                        List<Object> row = new ArrayList<>(columns.size());
                        for (int i = 1; i <= columns.size(); i++) {
                            row.add(rs.getObject(i));
                        }
                        batch.add(row);
                        if (batch.size() >= batchSize) {
                            writeBatch(ch, chClient, targetTable, columns, batch, metrics, args.clickHouseWriter);
                            batch.clear();
                        }
                    }
                    if (!batch.isEmpty()) {
                        writeBatch(ch, chClient, targetTable, columns, batch, metrics, args.clickHouseWriter);
                    }
                }
            }
            long verifyStart = System.nanoTime();
            long targetCount = ch.queryLong("SELECT count() FROM " + quoteCh(targetTable));
            metrics.verifyMs += elapsedMs(verifyStart);
            if (targetCount != metrics.rows) {
                throw new IllegalStateException("Target row count mismatch: expected "
                    + metrics.rows + ", got " + targetCount);
            }
            metrics.extra.put("columns", columns);
            metrics.extra.put("column_count", columns.size());
            metrics.extra.put("target_count", targetCount);
            metrics.extra.put("notes", "Spring Boot CLI with PostgreSQL streaming ResultSet and ClickHouse writer " + args.clickHouseWriter);
            metrics.extra.put("clickhouse_writer", args.clickHouseWriter);
        }
        if (chClient != null) {
            chClient.close();
        }

        metrics.totalMs = elapsedMs(started);
        metrics.cpuMs = currentThreadCpuMs() - cpuStarted;
        metrics.peakRssMb = usedHeapMb();
        metrics.finishRates();
        insertMetrics(ch, metrics);
        return metrics;
    }

    private static void prepareSource(Connection pg, Args args, Metrics metrics) throws Exception {
        if (args.file == null || args.file.isBlank()) {
            throw new IllegalArgumentException("--file is required unless --skip-prepare is used");
        }
        Path input = Path.of(args.file);
        if (!Files.exists(input)) {
            throw new IllegalArgumentException("Input file not found inside container: " + input);
        }
        String dataset = Identifiers.requireDataset(args.dataset);
        String table = dataset;
        String format = detectFormat(input);
        metrics.inputFile = input.toAbsolutePath().toString();
        metrics.fileFormat = format;
        long started = System.nanoTime();

        List<String> columns;
        long preparedRows;
        String prepareMode;

        if ("csv".equals(format) && args.limitRows == 0) {
            columns = readCsvColumns(input, args);
            createSourceTable(pg, table, columns);
            CopyManager copy = new CopyManager(pg.unwrap(BaseConnection.class));
            String copySql = "COPY oltp_source." + quotePg(table)
                + " (" + joinQuotedPg(columns) + ") FROM STDIN WITH (FORMAT csv, HEADER true"
                + copyDelimiterOption(args) + ")";
            try (InputStream in = new NulStrippingInputStream(Files.newInputStream(input))) {
                preparedRows = copy.copyIn(copySql, in);
            }
            prepareMode = "csv-direct-copy";
        } else {
            Path tempCsv = Files.createTempFile("etlbench-java-" + dataset + "-", ".csv");
            try {
            PreparedInput prepared = switch (format) {
                case "csv" -> materializeCsv(input, tempCsv, args);
                case "xlsx" -> materializeXlsx(input, tempCsv, args);
                default -> throw new IllegalArgumentException("Unsupported file format: " + input);
            };
            columns = prepared.columns;
            preparedRows = prepared.rows;

            createSourceTable(pg, table, columns);

            CopyManager copy = new CopyManager(pg.unwrap(BaseConnection.class));
            String copySql = "COPY oltp_source." + quotePg(table)
                + " (" + joinQuotedPg(columns) + ") FROM STDIN WITH (FORMAT csv, HEADER true)";
            try (InputStream in = Files.newInputStream(tempCsv)) {
                copy.copyIn(copySql, in);
            }
            prepareMode = "materialized-clean-csv";
            } finally {
            Files.deleteIfExists(tempCsv);
            }
        }

        metrics.prepareMs += elapsedMs(started);
        metrics.extra.put("source_columns", columns);
        metrics.extra.put("prepared_rows", preparedRows);
        metrics.extra.put("limit_rows", args.limitRows == 0 ? null : args.limitRows);
        metrics.extra.put("prepare_mode", prepareMode);
    }

    private static void createSourceTable(Connection pg, String table, List<String> columns) throws Exception {
        try (Statement st = pg.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS oltp_source");
            st.execute("DROP TABLE IF EXISTS oltp_source." + quotePg(table));
            st.execute(
                "CREATE TABLE oltp_source." + quotePg(table) + " ("
                    + "etl_row_num BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
                    + columnDdl(columns)
                    + ", loaded_at TIMESTAMPTZ DEFAULT now())"
            );
        }
    }

    private static List<String> readCsvColumns(Path input, Args args) throws Exception {
        CSVFormat readFormat = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setDelimiter(args.delimiter == null || args.delimiter.isEmpty() ? ',' : args.delimiter.charAt(0))
            .build();
        try (
            Reader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8);
            CSVParser parser = readFormat.parse(reader)
        ) {
            return normalizeHeaders(new ArrayList<>(parser.getHeaderMap().keySet()));
        }
    }

    private static String copyDelimiterOption(Args args) {
        if (args.delimiter == null || args.delimiter.isEmpty() || ",".equals(args.delimiter)) {
            return "";
        }
        if (args.delimiter.length() != 1) {
            throw new IllegalArgumentException("--delimiter must be a single character");
        }
        char delimiter = args.delimiter.charAt(0);
        if (delimiter == '\t') {
            return ", DELIMITER E'\\t'";
        }
        if (delimiter == '\'' || delimiter == '\\') {
            return ", DELIMITER E'\\" + delimiter + "'";
        }
        return ", DELIMITER '" + delimiter + "'";
    }

    private static PreparedInput materializeCsv(Path input, Path output, Args args) throws Exception {
        CSVFormat readFormat = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setDelimiter(args.delimiter == null || args.delimiter.isEmpty() ? ',' : args.delimiter.charAt(0))
            .build();
        try (
            Reader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8);
            CSVParser parser = readFormat.parse(reader);
            BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8);
            CSVPrinter printer = CSVFormat.DEFAULT.print(writer)
        ) {
            List<String> columns = normalizeHeaders(new ArrayList<>(parser.getHeaderMap().keySet()));
            printer.printRecord(columns);
            long rows = 0;
            for (CSVRecord record : parser) {
                if (args.limitRows > 0 && rows >= args.limitRows) {
                    break;
                }
                List<String> values = new ArrayList<>(columns.size());
                for (int i = 0; i < columns.size(); i++) {
                    values.add(cleanText(i < record.size() ? record.get(i) : null));
                }
                printer.printRecord(values);
                rows++;
            }
            return new PreparedInput(columns, rows);
        }
    }

    private static PreparedInput materializeXlsx(Path input, Path output, Args args) throws Exception {
        DataFormatter formatter = new DataFormatter();
        try (
            InputStream in = Files.newInputStream(input);
            Workbook workbook = WorkbookFactory.create(in);
            BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8);
            CSVPrinter printer = CSVFormat.DEFAULT.print(writer)
        ) {
            Sheet sheet = args.sheet == null || args.sheet.isBlank()
                ? workbook.getSheetAt(0)
                : workbook.getSheet(args.sheet);
            if (sheet == null) {
                throw new IllegalArgumentException("XLSX sheet not found: " + args.sheet);
            }
            Row headerRow = sheet.getRow(sheet.getFirstRowNum());
            if (headerRow == null) {
                throw new IllegalArgumentException("XLSX has no header row: " + input);
            }
            List<String> headers = new ArrayList<>();
            for (int i = 0; i < headerRow.getLastCellNum(); i++) {
                headers.add(formatter.formatCellValue(headerRow.getCell(i)));
            }
            List<String> columns = normalizeHeaders(headers);
            printer.printRecord(columns);
            long rows = 0;
            for (int r = sheet.getFirstRowNum() + 1; r <= sheet.getLastRowNum(); r++) {
                if (args.limitRows > 0 && rows >= args.limitRows) {
                    break;
                }
                Row row = sheet.getRow(r);
                List<String> values = new ArrayList<>(columns.size());
                for (int c = 0; c < columns.size(); c++) {
                    values.add(cleanText(row == null ? null : formatter.formatCellValue(row.getCell(c))));
                }
                printer.printRecord(values);
                rows++;
            }
            return new PreparedInput(columns, rows);
        }
    }

    private static List<String> pgColumns(Connection pg, String table) throws Exception {
        List<String> columns = new ArrayList<>();
        String sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'oltp_source'
              AND table_name = ?
              AND column_name <> 'loaded_at'
            ORDER BY ordinal_position
            """;
        try (PreparedStatement ps = pg.prepareStatement(sql)) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString(1));
                }
            }
        }
        if (columns.isEmpty()) {
            throw new IllegalStateException("No source columns found for oltp_source." + table);
        }
        return columns;
    }

    private static void createTarget(ClickHouseHttp ch, String table, List<String> columns, boolean truncate)
        throws Exception {
        ch.command("CREATE DATABASE IF NOT EXISTS " + quoteCh(ch.database));
        if (truncate) {
            ch.command("DROP TABLE IF EXISTS " + quoteCh(table));
        }
        List<String> ddl = new ArrayList<>();
        for (String column : columns) {
            if ("etl_row_num".equals(column)) {
                ddl.add(quoteCh(column) + " UInt64");
            } else {
                ddl.add(quoteCh(column) + " Nullable(String)");
            }
        }
        ch.command("CREATE TABLE IF NOT EXISTS " + quoteCh(table)
            + " (" + String.join(", ", ddl) + ") ENGINE = MergeTree ORDER BY etl_row_num");
        if (truncate) {
            ch.command("TRUNCATE TABLE " + quoteCh(table));
        }
    }

    private static void writeBatch(
        ClickHouseHttp ch,
        ClickHouseClientV3 chClient,
        String table,
        List<String> columns,
        List<List<Object>> rows,
        Metrics metrics,
        String writer
    )
        throws Exception {
        long loadStart = System.nanoTime();
        StringBuilder body = new StringBuilder();
        if ("http-tsv".equals(writer)) {
            body.append("INSERT INTO ").append(quoteCh(table)).append(" (")
                .append(joinQuotedCh(columns)).append(") FORMAT TabSeparated\n");
        }
        for (List<Object> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) {
                    body.append('\t');
                }
                Object value = row.get(i);
                body.append(toTabSeparated(value));
                if (value != null) {
                    metrics.logicalBytes += value.toString().getBytes(StandardCharsets.UTF_8).length;
                }
            }
            body.append('\n');
        }
        if ("client-v3".equals(writer)) {
            chClient.insert(table, body.toString());
        } else {
            ch.command(body.toString());
        }
        metrics.loadMs += elapsedMs(loadStart);
        metrics.rows += rows.size();
        metrics.batchCount += 1;
    }

    private static void insertMetrics(ClickHouseHttp ch, Metrics m) throws Exception {
        createMetricsTable(ch);
        List<String> columns = List.of(
            "run_id", "implementation", "dataset", "source_table", "target_table", "input_file", "file_format",
            "rows", "logical_bytes", "fetch_size", "batch_size", "batch_count", "prepare_ms", "extract_ms",
            "load_ms", "verify_ms", "total_ms", "rows_per_sec", "mb_per_sec", "peak_rss_mb", "cpu_ms", "extra_json"
        );
        List<Object> values = List.of(
            m.runId, m.implementation, m.dataset, m.sourceTable, m.targetTable, m.inputFile, m.fileFormat,
            m.rows, m.logicalBytes, m.fetchSize, m.batchSize, m.batchCount, m.prepareMs, m.extractMs,
            m.loadMs, m.verifyMs, m.totalMs, m.rowsPerSec, m.mbPerSec, m.peakRssMb, m.cpuMs, JSON.writeValueAsString(m.extra)
        );
        StringBuilder body = new StringBuilder();
        body.append("INSERT INTO benchmark_runs (").append(joinQuotedCh(columns)).append(") FORMAT TabSeparated\n");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                body.append('\t');
            }
            body.append(toTabSeparated(values.get(i)));
        }
        body.append('\n');
        ch.command(body.toString());
    }

    private static void createMetricsTable(ClickHouseHttp ch) throws Exception {
        ch.command("""
            CREATE TABLE IF NOT EXISTS benchmark_runs
            (
                run_id UUID,
                measured_at DateTime64(3, 'UTC') DEFAULT now64(3),
                implementation LowCardinality(String),
                dataset LowCardinality(String),
                source_table String,
                target_table String,
                input_file String,
                file_format LowCardinality(String),
                rows UInt64,
                logical_bytes UInt64,
                fetch_size UInt32,
                batch_size UInt32,
                batch_count UInt64,
                prepare_ms Float64,
                extract_ms Float64,
                load_ms Float64,
                verify_ms Float64,
                total_ms Float64,
                rows_per_sec Float64,
                mb_per_sec Float64,
                peak_rss_mb Float64,
                cpu_ms Float64,
                extra_json String
            )
            ENGINE = MergeTree
            ORDER BY (dataset, implementation, measured_at, run_id)
            """);
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }

    private static String detectFormat(Path input) {
        String name = input.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".csv")) {
            return "csv";
        }
        if (name.endsWith(".xlsx") || name.endsWith(".xlsm")) {
            return "xlsx";
        }
        throw new IllegalArgumentException("Unsupported input file format: " + input);
    }

    private static String columnDdl(List<String> columns) {
        return columns.stream()
            .map(column -> quotePg(column) + " TEXT")
            .reduce((a, b) -> a + ", " + b)
            .orElseThrow();
    }

    private static String cleanText(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        return value.replace("\u0000", "");
    }

    private static List<String> normalizeHeaders(List<String> headers) {
        List<String> result = new ArrayList<>();
        Map<String, Integer> seen = new HashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            String base = normalizeIdentifier(headers.get(i), "col_" + (i + 1));
            int count = seen.getOrDefault(base, 0) + 1;
            seen.put(base, count);
            if (count == 1) {
                result.add(base);
            } else {
                String suffix = "_" + count;
                result.add(base.substring(0, Math.min(base.length(), 63 - suffix.length())) + suffix);
            }
        }
        return result;
    }

    private static String normalizeIdentifier(String value, String fallback) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        normalized = normalized.replaceAll("[^a-z0-9_]+", "_").replaceAll("_+", "_").replaceAll("^_|_$", "");
        if (normalized.isBlank()) {
            normalized = fallback;
        }
        if (Character.isDigit(normalized.charAt(0))) {
            normalized = "col_" + normalized;
        }
        if (List.of("etl_row_num", "load_run_id", "loaded_at").contains(normalized)) {
            normalized = "src_" + normalized;
        }
        return normalized.length() > 63 ? normalized.substring(0, 63) : normalized;
    }

    private static String quotePg(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String quoteCh(String value) {
        return "`" + value.replace("`", "``") + "`";
    }

    private static String toTabSeparated(Object value) {
        if (value == null) {
            return "\\N";
        }
        return value.toString()
            .replace("\\", "\\\\")
            .replace("\t", "\\t")
            .replace("\n", "\\n")
            .replace("\r", "\\r");
    }

    private static String joinQuotedPg(List<String> values) {
        return values.stream().map(App::quotePg).reduce((a, b) -> a + ", " + b).orElse("");
    }

    private static String joinQuotedCh(List<String> values) {
        return values.stream().map(App::quoteCh).reduce((a, b) -> a + ", " + b).orElse("");
    }

    private static double elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000.0;
    }

    private static long currentThreadCpuMs() {
        var bean = ManagementFactory.getThreadMXBean();
        return bean.isCurrentThreadCpuTimeSupported() ? bean.getCurrentThreadCpuTime() / 1_000_000 : 0;
    }

    private static double usedHeapMb() {
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / 1024.0 / 1024.0;
    }

    static final class Args {
        String dataset;
        String file;
        String sheet;
        String delimiter;
        String clickHouseWriter = "http-tsv";
        int fetchSize = 50_000;
        int batchSize = 50_000;
        int limitRows;
        boolean skipPrepare;
        boolean truncateTarget = true;

        static Args parse(String[] raw) {
            Args args = new Args();
            for (int i = 0; i < raw.length; i++) {
                switch (raw[i]) {
                    case "--dataset" -> args.dataset = raw[++i];
                    case "--file" -> args.file = raw[++i];
                    case "--sheet" -> args.sheet = raw[++i];
                    case "--delimiter" -> args.delimiter = raw[++i];
                    case "--clickhouse-writer" -> args.clickHouseWriter = raw[++i];
                    case "--fetch-size" -> args.fetchSize = Integer.parseInt(raw[++i]);
                    case "--batch-size" -> args.batchSize = Integer.parseInt(raw[++i]);
                    case "--limit-rows" -> args.limitRows = Integer.parseInt(raw[++i]);
                    case "--skip-prepare" -> args.skipPrepare = true;
                    case "--keep-target" -> args.truncateTarget = false;
                    default -> throw new IllegalArgumentException("Unknown argument: " + raw[i]);
                }
            }
            if (args.dataset == null || args.dataset.isBlank()) {
                throw new IllegalArgumentException("--dataset is required");
            }
            if (!List.of("http-tsv", "client-v3").contains(args.clickHouseWriter)) {
                throw new IllegalArgumentException("--clickhouse-writer must be http-tsv or client-v3");
            }
            return args;
        }
    }

    record PreparedInput(List<String> columns, long rows) {
    }

    static final class NulStrippingInputStream extends FilterInputStream {
        NulStrippingInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws java.io.IOException {
            int value;
            do {
                value = super.read();
            } while (value == 0);
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws java.io.IOException {
            int read = super.read(bytes, offset, length);
            if (read <= 0) {
                return read;
            }
            int write = offset;
            for (int i = offset; i < offset + read; i++) {
                if (bytes[i] != 0) {
                    bytes[write++] = bytes[i];
                }
            }
            return write - offset;
        }
    }

    static final class Identifiers {
        static String requireDataset(String value) {
            String normalized = value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]+", "_");
            if (!List.of("fact_animals", "med_technic", "turkestan_109_incidents").contains(normalized)) {
                throw new IllegalArgumentException("Unknown dataset: " + value);
            }
            return normalized;
        }
    }

    static final class Metrics {
        String runId = UUID.randomUUID().toString();
        String implementation;
        String dataset;
        String sourceTable;
        String targetTable;
        String inputFile = "";
        String fileFormat = "";
        long rows;
        long logicalBytes;
        int fetchSize;
        int batchSize;
        long batchCount;
        double prepareMs;
        double extractMs;
        double loadMs;
        double verifyMs;
        double totalMs;
        double rowsPerSec;
        double mbPerSec;
        double peakRssMb;
        double cpuMs;
        Map<String, Object> extra = new LinkedHashMap<>();

        void finishRates() {
            double seconds = totalMs / 1000.0;
            rowsPerSec = seconds > 0 ? rows / seconds : 0;
            mbPerSec = seconds > 0 ? (logicalBytes / 1024.0 / 1024.0) / seconds : 0;
        }

        Map<String, Object> toMap() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("run_id", runId);
            data.put("measured_at", Instant.now().toString());
            data.put("implementation", implementation);
            data.put("dataset", dataset);
            data.put("source_table", sourceTable);
            data.put("target_table", targetTable);
            data.put("input_file", inputFile);
            data.put("file_format", fileFormat);
            data.put("rows", rows);
            data.put("logical_bytes", logicalBytes);
            data.put("fetch_size", fetchSize);
            data.put("batch_size", batchSize);
            data.put("batch_count", batchCount);
            data.put("prepare_ms", prepareMs);
            data.put("extract_ms", extractMs);
            data.put("load_ms", loadMs);
            data.put("verify_ms", verifyMs);
            data.put("total_ms", totalMs);
            data.put("rows_per_sec", rowsPerSec);
            data.put("mb_per_sec", mbPerSec);
            data.put("peak_rss_mb", peakRssMb);
            data.put("cpu_ms", cpuMs);
            data.put("extra", extra);
            return data;
        }
    }

    static final class ClickHouseHttp {
        final String database;
        private final HttpClient client = HttpClient.newHttpClient();
        private final String endpoint;
        private final String authHeader;

        ClickHouseHttp(String host, int port, String database, String user, String password) {
            this.database = database;
            this.endpoint = "http://" + host + ":" + port + "/?database="
                + URLEncoder.encode(database, StandardCharsets.UTF_8);
            this.authHeader = "Basic " + Base64.getEncoder()
                .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
        }

        void command(String sql) throws Exception {
            HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint))
                .header("Authorization", authHeader)
                .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8))
                .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 300) {
                throw new IllegalStateException("ClickHouse HTTP " + response.statusCode() + ": " + response.body());
            }
        }

        long queryLong(String sql) throws Exception {
            HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint))
                .header("Authorization", authHeader)
                .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8))
                .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 300) {
                throw new IllegalStateException("ClickHouse HTTP " + response.statusCode() + ": " + response.body());
            }
            return Long.parseLong(response.body().trim());
        }
    }

    static final class ClickHouseClientV3 implements AutoCloseable {
        private final Client client;
        private final InsertSettings insertSettings = new InsertSettings().setInputStreamCopyBufferSize(1024 * 1024);

        ClickHouseClientV3(String host, int port, String database, String user, String password) {
            this.client = new Client.Builder()
                .addEndpoint("http://" + host + ":" + port + "/")
                .setDefaultDatabase(database)
                .setUsername(user)
                .setPassword(password)
                .build();
        }

        void insert(String table, String tsvBody) throws Exception {
            try (
                ByteArrayInputStream input = new ByteArrayInputStream(tsvBody.getBytes(StandardCharsets.UTF_8));
                InsertResponse response = client.insert(table, input, ClickHouseFormat.TSV, insertSettings).get(5, TimeUnit.MINUTES)
            ) {
                // Closing the response releases the client connection.
            }
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
