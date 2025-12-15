package ca.digilogue.xp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.kafka.annotation.EnableKafka;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import ca.digilogue.xp.generator.OhlcvCandle;

@SpringBootApplication
@EnableKafka
public class App {

    public static String version;
    public static String instanceId;

    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static ConfigurableApplicationContext applicationContext;
    
    public static final Map<String, OhlcvCandle> latestCandles = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        applicationContext = SpringApplication.run(App.class, args);

        version = resolveVersion(applicationContext);
        instanceId = resolveInstanceId();

        log.info("xp-marketdata-service is running @ version: {}, instanceId: {}", version, instanceId);
    }

    private static String resolveVersion(ConfigurableApplicationContext ctx) {
        // Preferred: use build-info (packaged JAR / Docker)
        try {
            BuildProperties buildProperties = ctx.getBean(BuildProperties.class);
            return buildProperties.getVersion();
        } catch (NoSuchBeanDefinitionException ignored) {
            // Fall back to parsing pom.xml (e.g., when running from IDE)
        }

        return readVersionFromPom();
    }

    private static String readVersionFromPom() {
        try {
            List<String> lines = Files.readAllLines(Paths.get("pom.xml"));

            boolean inParent = false;

            for (String line : lines) {
                String trimmed = line.trim();

                // Track whether we're inside the <parent>...</parent> block
                if (trimmed.startsWith("<parent>")) {
                    inParent = true;
                } else if (trimmed.startsWith("</parent>")) {
                    inParent = false;
                }

                // We only care about <version> tags OUTSIDE the <parent> section
                if (!inParent &&
                        trimmed.startsWith("<version>") &&
                        trimmed.endsWith("</version>")) {

                    return trimmed
                            .replace("<version>", "")
                            .replace("</version>", "")
                            .trim();
                }
            }
        } catch (Exception ignored) {
            // If something goes wrong reading pom.xml, fall through
        }

        // Last-resort fallback for local dev
        return "DEV";
    }

    private static String resolveInstanceId() {
        // First, try environment variable (set in docker-compose)
        String envInstanceId = System.getenv("INSTANCE_ID");
        if (envInstanceId != null && !envInstanceId.isEmpty()) {
            return envInstanceId;
        }

        // Fallback to hostname (Docker container name)
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isEmpty()) {
            return hostname;
        }

        // Last resort: generate a UUID
        return java.util.UUID.randomUUID().toString().substring(0, 8);
    }
}
