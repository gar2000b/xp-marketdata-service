package ca.digilogue.xp.config;

import ca.digilogue.xp.generator.OhlcvCandle;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration for consuming OHLCV candles collection.
 * Consumes the entire Map<String, OhlcvCandle> as a single JSON message from the ohlcv-topic.
 * Uses custom CandlesMapDeserializer for proper Map deserialization.
 * 
 * Configured for live streaming: always consumes NEW messages as they arrive.
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:xp-marketdata-service-group}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, Map<String, OhlcvCandle>> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // Consumer settings for live streaming
        // Always start from latest offset - consume NEW messages as they arrive
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // Session and heartbeat timeouts
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        
        log.info("Creating ConsumerFactory for live streaming:");
        log.info("  bootstrap-servers: {}", bootstrapServers);
        log.info("  group-id: {}", groupId);
        log.info("  auto-offset-reset: latest (consume NEW messages only)");
        log.info("  enable-auto-commit: false (no offset commits)");
        
        // Use custom deserializer for Map<String, OhlcvCandle>
        CandlesMapDeserializer candlesMapDeserializer = new CandlesMapDeserializer();
        
        return new DefaultKafkaConsumerFactory<>(configProps, 
            new StringDeserializer(), 
            candlesMapDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Map<String, OhlcvCandle>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Map<String, OhlcvCandle>> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Set acknowledgment mode to manual since auto-commit is disabled
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        log.info("Created kafkaListenerContainerFactory with manual acknowledgment mode");
        log.info("Container will consume messages from ohlcv-topic as they arrive");
        
        return factory;
    }
}
