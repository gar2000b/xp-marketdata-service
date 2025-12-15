package ca.digilogue.xp.service;

import ca.digilogue.xp.App;
import ca.digilogue.xp.generator.OhlcvCandle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Kafka consumer service for consuming OHLCV candles collection from Kafka.
 * Updates the static latestCandles map in App.java with the consumed data.
 * 
 * Implements ConsumerSeekAware to explicitly seek to the end of partitions
 * on startup, ensuring only NEW messages are consumed (live streaming).
 */
@Service
public class KafkaConsumerService implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Value("${spring.kafka.topic.ohlcv:ohlcv-topic}")
    private String topicName;

    @Value("${spring.kafka.consumer.group-id:xp-marketdata-service-group}")
    private String groupId;

    @PostConstruct
    public void init() {
        log.info("========================================");
        log.info("KafkaConsumerService initialized");
        log.info("  Topic: '{}'", topicName);
        log.info("  Group ID: '{}'", groupId);
        log.info("  Container Factory: 'kafkaListenerContainerFactory'");
        log.info("  Auto-offset-reset: latest (will only consume NEW messages after consumer starts)");
        log.info("  ConsumerSeekAware: Will explicitly seek to END of partitions on startup");
        log.info("========================================");
    }

    /**
     * Called when partitions are assigned to this consumer.
     * Explicitly seeks to the END of each partition to ensure we only consume NEW messages.
     * This overrides any committed offsets and ensures live streaming behavior.
     */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.info("Partitions assigned: {}", assignments.keySet());
        for (TopicPartition partition : assignments.keySet()) {
            log.info("Seeking to END of partition: {} (current offset: {})", partition, assignments.get(partition));
            callback.seekToEnd(partition.topic(), partition.partition());
        }
        log.info("All {} partition(s) seeked to END - consumer will only process NEW messages", assignments.size());
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        // Not needed for our use case
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        // Not needed for our use case
    }

    /**
     * Consumes OHLCV candles collection from Kafka topic.
     * Updates the static latestCandles map in App.java.
     * 
     * @param candles        Map of symbol to OHLCV candle (the entire collection)
     * @param acknowledgment Kafka acknowledgment for manual commit (if needed)
     */
    @KafkaListener(
        topics = "${spring.kafka.topic.ohlcv:ohlcv-topic}", 
        groupId = "${spring.kafka.consumer.group-id:xp-marketdata-service-group}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeCandlesCollection(
            @Payload Map<String, OhlcvCandle> candles,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("=== RECEIVED candles collection from topic: {}, partition: {}, offset: {} ===",
                topic, partition, offset);

        try {
            if (candles == null || candles.isEmpty()) {
                log.info("Received empty candles collection from topic: {}, partition: {}, offset: {}",
                        topic, partition, offset);
                return;
            }

            // Update the static latestCandles map in App.java
            synchronized (App.latestCandles) {
                App.latestCandles.clear();
                App.latestCandles.putAll(candles);
            }

            // Serialize candles to JSON for logging
            String candlesJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(candles);
            
            log.info("Consumed and updated {} candles from topic: {}, partition: {}, offset: {}\nPayload JSON:\n{}",
                    candles.size(), topic, partition, offset, candlesJson);

            // Acknowledge the message (if manual acknowledgment is enabled)
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }

        } catch (Exception e) {
            log.error("Error consuming candles collection from topic: {}, partition: {}, offset: {}",
                    topic, partition, offset, e);
            // Don't acknowledge on error - let Kafka retry
        }
    }
}
