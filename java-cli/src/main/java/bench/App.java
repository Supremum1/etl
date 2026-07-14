package bench;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    public void run(String... rawArgs) throws Exception {
        Metrics metrics = new BenchmarkPipeline().run(CliArgs.parse(rawArgs));
        System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(metrics.toMap()));
    }
}
