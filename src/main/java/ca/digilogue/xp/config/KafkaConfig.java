package ca.digilogue.xp.config;

import ca.digilogue.xp.generator.OhlcvCandle;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration for consuming OHLCV candles collection.
 * Consumes the entire Map<String, OhlcvCandle> as a single JSON message from the ohlcv-topic.
 * Uses custom CandlesMapDeserializer for proper Map deserialization.
 */
@Configuration
public class KafkaConfig {

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
        // VALUE_DESERIALIZER_CLASS_CONFIG is not needed when using custom deserializer instance
        
        // Consumer settings
        // Always start from latest offset (ignore committed offsets)
        // By disabling auto-commit, no offsets are saved, so every restart starts from latest
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
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
        // With ENABLE_AUTO_COMMIT_CONFIG = false, offsets won't be committed
        // This ensures every restart starts from latest (no committed offsets)
        // No AckMode setting needed - default behavior with auto-commit disabled prevents commits
        return factory;
    }
}
