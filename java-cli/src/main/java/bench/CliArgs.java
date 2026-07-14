package bench;

final class CliArgs {
    String dataset;
    String file;
    String sheet;
    String delimiter;
    int fetchSize = 50_000;
    int batchSize = 50_000;
    int limitRows;
    boolean skipPrepare;
    boolean truncateTarget = true;

    static CliArgs parse(String[] raw) {
        CliArgs args = new CliArgs();
        for (int i = 0; i < raw.length; i++) {
            switch (raw[i]) {
                case "--dataset" -> args.dataset = raw[++i];
                case "--file" -> args.file = raw[++i];
                case "--sheet" -> args.sheet = raw[++i];
                case "--delimiter" -> args.delimiter = raw[++i];
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
        if (args.fetchSize <= 0 || args.batchSize <= 0) {
            throw new IllegalArgumentException("--fetch-size and --batch-size must be positive");
        }
        return args;
    }
}
