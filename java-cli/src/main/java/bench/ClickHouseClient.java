package bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

final class ClickHouseClient implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Connection connection;
    private final String database;
    private PreparedStatement targetInsert;

    ClickHouseClient(ClickHouseSettings settings) throws Exception {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Properties properties = new Properties();
        properties.setProperty("user", settings.user());
        properties.setProperty("password", settings.password());
        connection = DriverManager.getConnection(settings.url(), properties);
        database = settings.database();
    }

    String database() {
        return database;
    }

    void createTarget(String table, List<String> columns, boolean truncate) throws Exception {
        closeTargetInsert();
        try (Statement statement = connection.createStatement()) {
            if (truncate) {
                statement.execute("DROP TABLE IF EXISTS " + SqlNames.quoteCh(table));
            }
            List<String> ddl = new ArrayList<>();
            for (String column : columns) {
                String type = "etl_row_num".equals(column) ? "UInt64" : "Nullable(String)";
                ddl.add(SqlNames.quoteCh(column) + " " + type);
            }
            statement.execute(
                "CREATE TABLE IF NOT EXISTS " + SqlNames.quoteCh(table)
                    + " (" + String.join(", ", ddl) + ") ENGINE = MergeTree ORDER BY etl_row_num"
            );
        }

        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));
        String quotedColumns = columns.stream()
            .map(SqlNames::quoteCh)
            .reduce((a, b) -> a + ", " + b)
            .orElseThrow();
        targetInsert = connection.prepareStatement(
            "INSERT INTO " + SqlNames.quoteCh(table) + " (" + quotedColumns + ") VALUES ("
                + placeholders + ")"
        );
    }

    void writeBatch(List<List<Object>> rows, Metrics metrics) throws Exception {
        if (targetInsert == null) {
            throw new IllegalStateException("ClickHouse target insert is not prepared");
        }
        for (List<Object> row : rows) {
            for (Object value : row) {
                if (value != null) {
                    metrics.logicalBytes += value.toString().getBytes(StandardCharsets.UTF_8).length;
                }
            }
        }

        long loadStarted = System.nanoTime();
        for (List<Object> row : rows) {
            targetInsert.setLong(1, ((Number) row.get(0)).longValue());
            for (int column = 1; column < row.size(); column++) {
                Object value = row.get(column);
                if (value == null) {
                    targetInsert.setNull(column + 1, Types.VARCHAR);
                } else {
                    targetInsert.setString(column + 1, value.toString());
                }
            }
            targetInsert.addBatch();
        }
        targetInsert.executeBatch();
        targetInsert.clearBatch();
        metrics.loadMs += ProcessMetrics.elapsedMs(loadStarted);
        metrics.rows += rows.size();
        metrics.batchCount++;
    }

    long count(String table) throws Exception {
        try (
            Statement statement = connection.createStatement();
            ResultSet row = statement.executeQuery("SELECT count() FROM " + SqlNames.quoteCh(table))
        ) {
            row.next();
            return row.getLong(1);
        }
    }

    void insertMetrics(Metrics metrics) throws Exception {
        createMetricsTable();
        String sql = """
            INSERT INTO benchmark_runs
            (benchmark_version, benchmark_mode, run_id, implementation, dataset, source_table,
             target_table, input_file, file_format, rows, logical_bytes, fetch_size, batch_size,
             batch_count, prepare_ms, source_verify_ms, target_setup_ms, extract_ms, serialize_ms,
             load_ms, verify_ms, overhead_ms, total_ms, rows_per_sec, mb_per_sec, peak_rss_mb,
             cpu_ms, extra_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement insert = connection.prepareStatement(sql)) {
            List<Object> values = List.of(
                metrics.benchmarkVersion, metrics.benchmarkMode, metrics.runId, metrics.implementation,
                metrics.dataset, metrics.sourceTable, metrics.targetTable, metrics.inputFile,
                metrics.fileFormat, metrics.rows, metrics.logicalBytes, metrics.fetchSize,
                metrics.batchSize, metrics.batchCount, metrics.prepareMs, metrics.sourceVerifyMs,
                metrics.targetSetupMs, metrics.extractMs, metrics.serializeMs, metrics.loadMs,
                metrics.verifyMs, metrics.overheadMs, metrics.totalMs, metrics.rowsPerSec,
                metrics.mbPerSec, metrics.peakRssMb, metrics.cpuMs,
                JSON.writeValueAsString(metrics.extra)
            );
            for (int index = 0; index < values.size(); index++) {
                insert.setObject(index + 1, values.get(index));
            }
            insert.executeUpdate();
        }
    }

    private void createMetricsTable() throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                CREATE TABLE IF NOT EXISTS benchmark_runs
                (
                    benchmark_version UInt16 DEFAULT 4,
                    benchmark_mode LowCardinality(String),
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
                    source_verify_ms Float64,
                    target_setup_ms Float64,
                    extract_ms Float64,
                    serialize_ms Float64,
                    load_ms Float64,
                    verify_ms Float64,
                    overhead_ms Float64,
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
            statement.execute(
                "ALTER TABLE benchmark_runs ADD COLUMN IF NOT EXISTS benchmark_version UInt16 DEFAULT 1"
            );
            statement.execute(
                "ALTER TABLE benchmark_runs ADD COLUMN IF NOT EXISTS benchmark_mode "
                    + "LowCardinality(String) DEFAULT 'legacy'"
            );
            for (String column : List.of(
                "source_verify_ms", "target_setup_ms", "serialize_ms", "overhead_ms"
            )) {
                statement.execute(
                    "ALTER TABLE benchmark_runs ADD COLUMN IF NOT EXISTS " + column
                        + " Float64 DEFAULT 0"
                );
            }
        }
    }

    private void closeTargetInsert() throws Exception {
        if (targetInsert != null) {
            targetInsert.close();
            targetInsert = null;
        }
    }

    @Override
    public void close() throws Exception {
        closeTargetInsert();
        connection.close();
    }
}
