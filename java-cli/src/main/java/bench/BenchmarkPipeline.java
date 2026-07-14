package bench;

import java.util.ArrayList;
import java.util.List;

final class BenchmarkPipeline {
    Metrics run(CliArgs args) throws Exception {
        String dataset = SqlNames.requireDataset(args.dataset);
        String sourceTable = dataset;
        String targetTable = dataset + "_olap";
        long started = System.nanoTime();
        long cpuStarted = ProcessMetrics.cpuMs();

        Metrics metrics = new Metrics();
        metrics.benchmarkMode = args.skipPrepare ? "transfer" : "full";
        metrics.implementation = "java-spring-jdbc";
        metrics.dataset = dataset;
        metrics.sourceTable = "oltp_source." + sourceTable;
        metrics.fetchSize = args.fetchSize;
        metrics.batchSize = args.batchSize;

        ProcessMetrics.MemorySampler memory = new ProcessMetrics.MemorySampler();
        try (
            memory;
            PostgresClient postgres = new PostgresClient(PostgresSettings.fromEnvironment());
            ClickHouseClient clickhouse = new ClickHouseClient(ClickHouseSettings.fromEnvironment())
        ) {
            metrics.targetTable = clickhouse.database() + "." + targetTable;
            if (!args.skipPrepare) {
                postgres.prepareSource(args, metrics);
            }

            long targetSetupStarted = System.nanoTime();
            List<String> columns = postgres.columns(sourceTable);
            clickhouse.createTarget(targetTable, columns, args.truncateTarget);
            metrics.targetSetupMs += ProcessMetrics.elapsedMs(targetSetupStarted);

            postgres.readPages(sourceTable, columns, args.fetchSize, metrics, page -> {
                for (int offset = 0; offset < page.size(); offset += args.batchSize) {
                    int end = Math.min(offset + args.batchSize, page.size());
                    clickhouse.writeBatch(new ArrayList<>(page.subList(offset, end)), metrics);
                }
            });

            long verifyStarted = System.nanoTime();
            long targetCount = clickhouse.count(targetTable);
            metrics.verifyMs += ProcessMetrics.elapsedMs(verifyStarted);
            if (targetCount != metrics.rows) {
                throw new IllegalStateException(
                    "Target row count mismatch: expected " + metrics.rows + ", got " + targetCount
                );
            }

            metrics.totalMs = ProcessMetrics.elapsedMs(started);
            metrics.cpuMs = ProcessMetrics.cpuMs() - cpuStarted;
            metrics.peakRssMb = memory.peakRssMb();
            double measuredMs = metrics.prepareMs + metrics.sourceVerifyMs + metrics.targetSetupMs
                + metrics.extractMs + metrics.serializeMs + metrics.loadMs + metrics.verifyMs;
            metrics.overheadMs = Math.max(0, metrics.totalMs - measuredMs);
            metrics.finishRates();
            metrics.extra.put("columns", columns);
            metrics.extra.put("column_count", columns.size());
            metrics.extra.put("target_count", targetCount);
            metrics.extra.put("clickhouse_client", "housepower-native-jdbc");
            metrics.extra.put("clickhouse_transport", "Native protocol over TCP port 9000");
            metrics.extra.put(
                "notes",
                "One PostgreSQL streaming ResultSet -> ClickHouse Native columnar blocks"
            );
            clickhouse.insertMetrics(metrics);
        }
        return metrics;
    }
}
