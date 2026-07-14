package bench;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

final class Metrics {
    int benchmarkVersion = 4;
    String benchmarkMode = "full";
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
    double sourceVerifyMs;
    double targetSetupMs;
    double extractMs;
    double serializeMs;
    double loadMs;
    double verifyMs;
    double overheadMs;
    double totalMs;
    double rowsPerSec;
    double mbPerSec;
    double peakRssMb;
    double cpuMs;
    final Map<String, Object> extra = new LinkedHashMap<>();

    void finishRates() {
        double seconds = totalMs / 1000.0;
        rowsPerSec = seconds > 0 ? rows / seconds : 0;
        mbPerSec = seconds > 0 ? (logicalBytes / 1024.0 / 1024.0) / seconds : 0;
    }

    Map<String, Object> toMap() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("benchmark_version", benchmarkVersion);
        data.put("benchmark_mode", benchmarkMode);
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
        data.put("source_verify_ms", sourceVerifyMs);
        data.put("target_setup_ms", targetSetupMs);
        data.put("extract_ms", extractMs);
        data.put("serialize_ms", serializeMs);
        data.put("load_ms", loadMs);
        data.put("verify_ms", verifyMs);
        data.put("overhead_ms", overheadMs);
        data.put("total_ms", totalMs);
        data.put("rows_per_sec", rowsPerSec);
        data.put("mb_per_sec", mbPerSec);
        data.put("peak_rss_mb", peakRssMb);
        data.put("cpu_ms", cpuMs);
        data.put("extra", extra);
        return data;
    }
}
