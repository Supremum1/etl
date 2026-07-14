package bench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class SqlNames {
    private SqlNames() {
    }

    static String requireDataset(String value) {
        String normalized = normalizeIdentifier(value, "dataset");
        if (!List.of("fact_animals", "med_technic", "turkestan_109_incidents").contains(normalized)) {
            throw new IllegalArgumentException("Unknown dataset: " + value);
        }
        return normalized;
    }

    static List<String> normalizeHeaders(List<String> headers) {
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

    static String quotePg(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    static String quoteCh(String value) {
        return "`" + value.replace("`", "``") + "`";
    }

    static String joinQuotedPg(List<String> values) {
        return values.stream().map(SqlNames::quotePg).reduce((a, b) -> a + ", " + b).orElse("");
    }

    private static String normalizeIdentifier(String value, String fallback) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        normalized = normalized.replaceAll("[^a-z0-9_]+", "_")
            .replaceAll("_+", "_").replaceAll("^_|_$", "");
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
}
