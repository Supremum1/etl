package bench;

import java.io.BufferedWriter;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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

final class PostgresClient implements AutoCloseable {
    private final Connection connection;

    PostgresClient(PostgresSettings settings) throws Exception {
        connection = DriverManager.getConnection(settings.url(), settings.user(), settings.password());
    }

    void prepareSource(CliArgs args, Metrics metrics) throws Exception {
        if (args.file == null || args.file.isBlank()) {
            throw new IllegalArgumentException("--file is required unless --skip-prepare is used");
        }
        Path input = Path.of(args.file);
        if (!Files.exists(input)) {
            throw new IllegalArgumentException("Input file not found inside container: " + input);
        }

        String table = SqlNames.requireDataset(args.dataset);
        String format = detectFormat(input);
        metrics.inputFile = input.toAbsolutePath().toString();
        metrics.fileFormat = format;
        long started = System.nanoTime();
        List<String> columns;
        String prepareMode;

        if ("csv".equals(format) && args.limitRows == 0) {
            columns = readCsvColumns(input, args);
            createSourceTable(table, columns);
            copyCsv(input, table, columns, copyDelimiterOption(args));
            prepareMode = "csv-direct-copy";
        } else {
            Path tempCsv = Files.createTempFile("etlbench-java-" + table + "-", ".csv");
            try {
                PreparedInput prepared = switch (format) {
                    case "csv" -> materializeCsv(input, tempCsv, args);
                    case "xlsx" -> materializeXlsx(input, tempCsv, args);
                    default -> throw new IllegalArgumentException("Unsupported file format: " + input);
                };
                columns = prepared.columns();
                createSourceTable(table, columns);
                copyCsv(tempCsv, table, columns, "");
                prepareMode = "materialized-clean-csv";
            } finally {
                Files.deleteIfExists(tempCsv);
            }
        }

        metrics.prepareMs += ProcessMetrics.elapsedMs(started);
        long verifyStarted = System.nanoTime();
        long preparedRows = countSource(table);
        metrics.sourceVerifyMs += ProcessMetrics.elapsedMs(verifyStarted);
        metrics.extra.put("source_columns", columns);
        metrics.extra.put("prepared_rows", preparedRows);
        metrics.extra.put("limit_rows", args.limitRows == 0 ? null : args.limitRows);
        metrics.extra.put("prepare_mode", prepareMode);
    }

    List<String> columns(String table) throws Exception {
        List<String> columns = new ArrayList<>();
        String sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'oltp_source'
              AND table_name = ?
              AND column_name <> 'loaded_at'
            ORDER BY ordinal_position
            """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, table);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    columns.add(rows.getString(1));
                }
            }
        }
        if (columns.isEmpty()) {
            throw new IllegalStateException("No source columns found for oltp_source." + table);
        }
        return columns;
    }

    void readPages(
        String table,
        List<String> columns,
        int fetchSize,
        Metrics metrics,
        PageConsumer consumer
    ) throws Exception {
        String sql = "SELECT " + SqlNames.joinQuotedPg(columns) + " FROM oltp_source."
            + SqlNames.quotePg(table) + " ORDER BY etl_row_num";
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(
            sql,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY
        )) {
            statement.setFetchSize(fetchSize);
            try (ResultSet rows = statement.executeQuery()) {
                boolean hasMore = true;
                while (hasMore) {
                    long extractStarted = System.nanoTime();
                    List<List<Object>> page = new ArrayList<>(fetchSize);
                    while (page.size() < fetchSize && (hasMore = rows.next())) {
                        List<Object> row = new ArrayList<>(columns.size());
                        for (int i = 1; i <= columns.size(); i++) {
                            row.add(rows.getObject(i));
                        }
                        page.add(row);
                    }
                    metrics.extractMs += ProcessMetrics.elapsedMs(extractStarted);
                    if (!page.isEmpty()) {
                        consumer.accept(page);
                    }
                }
            }
        } finally {
            connection.rollback();
            connection.setAutoCommit(true);
        }
    }

    private void createSourceTable(String table, List<String> columns) throws Exception {
        String columnDdl = columns.stream()
            .map(column -> SqlNames.quotePg(column) + " TEXT")
            .reduce((a, b) -> a + ", " + b)
            .orElseThrow();
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS oltp_source");
            statement.execute("DROP TABLE IF EXISTS oltp_source." + SqlNames.quotePg(table));
            statement.execute(
                "CREATE TABLE oltp_source." + SqlNames.quotePg(table) + " ("
                    + "etl_row_num BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
                    + columnDdl + ", loaded_at TIMESTAMPTZ DEFAULT now())"
            );
        }
    }

    private void copyCsv(Path input, String table, List<String> columns, String delimiterOption)
        throws Exception {
        CopyManager copy = new CopyManager(connection.unwrap(BaseConnection.class));
        String copySql = "COPY oltp_source." + SqlNames.quotePg(table)
            + " (" + SqlNames.joinQuotedPg(columns) + ") FROM STDIN WITH (FORMAT csv, HEADER true"
            + delimiterOption + ")";
        try (InputStream stream = new NulStrippingInputStream(Files.newInputStream(input))) {
            copy.copyIn(copySql, stream);
        }
    }

    private long countSource(String table) throws Exception {
        try (
            Statement statement = connection.createStatement();
            ResultSet row = statement.executeQuery(
                "SELECT count(*) FROM oltp_source." + SqlNames.quotePg(table)
            )
        ) {
            row.next();
            return row.getLong(1);
        }
    }

    private static List<String> readCsvColumns(Path input, CliArgs args) throws Exception {
        CSVFormat format = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setDelimiter(delimiter(args))
            .build();
        try (
            Reader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8);
            CSVParser parser = format.parse(reader)
        ) {
            return SqlNames.normalizeHeaders(new ArrayList<>(parser.getHeaderMap().keySet()));
        }
    }

    private static PreparedInput materializeCsv(Path input, Path output, CliArgs args) throws Exception {
        CSVFormat format = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setDelimiter(delimiter(args))
            .build();
        try (
            Reader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8);
            CSVParser parser = format.parse(reader);
            BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8);
            CSVPrinter printer = CSVFormat.DEFAULT.print(writer)
        ) {
            List<String> columns = SqlNames.normalizeHeaders(new ArrayList<>(parser.getHeaderMap().keySet()));
            printer.printRecord(columns);
            long rowCount = 0;
            for (CSVRecord record : parser) {
                if (args.limitRows > 0 && rowCount >= args.limitRows) {
                    break;
                }
                List<String> values = new ArrayList<>(columns.size());
                for (int i = 0; i < columns.size(); i++) {
                    values.add(cleanText(i < record.size() ? record.get(i) : null));
                }
                printer.printRecord(values);
                rowCount++;
            }
            return new PreparedInput(columns, rowCount);
        }
    }

    private static PreparedInput materializeXlsx(Path input, Path output, CliArgs args) throws Exception {
        DataFormatter formatter = new DataFormatter();
        try (
            InputStream stream = Files.newInputStream(input);
            Workbook workbook = WorkbookFactory.create(stream);
            BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8);
            CSVPrinter printer = CSVFormat.DEFAULT.print(writer)
        ) {
            Sheet sheet = args.sheet == null || args.sheet.isBlank()
                ? workbook.getSheetAt(0)
                : workbook.getSheet(args.sheet);
            if (sheet == null) {
                throw new IllegalArgumentException("XLSX sheet not found: " + args.sheet);
            }
            Row header = sheet.getRow(sheet.getFirstRowNum());
            if (header == null) {
                throw new IllegalArgumentException("XLSX has no header row: " + input);
            }
            List<String> headers = new ArrayList<>();
            for (int i = 0; i < header.getLastCellNum(); i++) {
                headers.add(formatter.formatCellValue(header.getCell(i)));
            }
            List<String> columns = SqlNames.normalizeHeaders(headers);
            printer.printRecord(columns);
            long rowCount = 0;
            for (int index = sheet.getFirstRowNum() + 1; index <= sheet.getLastRowNum(); index++) {
                if (args.limitRows > 0 && rowCount >= args.limitRows) {
                    break;
                }
                Row row = sheet.getRow(index);
                List<String> values = new ArrayList<>(columns.size());
                for (int column = 0; column < columns.size(); column++) {
                    values.add(cleanText(
                        row == null ? null : formatter.formatCellValue(row.getCell(column))
                    ));
                }
                printer.printRecord(values);
                rowCount++;
            }
            return new PreparedInput(columns, rowCount);
        }
    }

    private static char delimiter(CliArgs args) {
        return args.delimiter == null || args.delimiter.isEmpty() ? ',' : args.delimiter.charAt(0);
    }

    private static String copyDelimiterOption(CliArgs args) {
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

    private static String detectFormat(Path input) {
        String name = input.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".csv")) {
            return "csv";
        }
        if (name.endsWith(".xlsx")) {
            return "xlsx";
        }
        throw new IllegalArgumentException("Expected .csv or .xlsx file: " + input);
    }

    private static String cleanText(String value) {
        return value == null || value.isEmpty() ? null : value.replace("\u0000", "");
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @FunctionalInterface
    interface PageConsumer {
        void accept(List<List<Object>> page) throws Exception;
    }

    private record PreparedInput(List<String> columns, long rows) {
    }

    private static final class NulStrippingInputStream extends FilterInputStream {
        NulStrippingInputStream(InputStream input) {
            super(input);
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
}
