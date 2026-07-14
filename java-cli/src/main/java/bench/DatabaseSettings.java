package bench;

record PostgresSettings(String url, String user, String password) {
    static PostgresSettings fromEnvironment() {
        String url = "jdbc:postgresql://" + env("POSTGRES_HOST", "localhost") + ":"
            + env("POSTGRES_PORT", "5432") + "/" + env("POSTGRES_DB", "etl_source");
        return new PostgresSettings(url, env("POSTGRES_USER", "etl"), env("POSTGRES_PASSWORD", "etl"));
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }
}

record ClickHouseSettings(String url, String database, String user, String password) {
    static ClickHouseSettings fromEnvironment() {
        String database = env("CLICKHOUSE_DATABASE", "olap_benchmark");
        String url = "jdbc:clickhouse://" + env("CLICKHOUSE_HOST", "localhost") + ":"
            + env("CLICKHOUSE_PORT", "9000") + "/" + database;
        return new ClickHouseSettings(
            url,
            database,
            env("CLICKHOUSE_USER", "default"),
            env("CLICKHOUSE_PASSWORD", "etl")
        );
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }
}
