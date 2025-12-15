package ca.digilogue.xp.service;

import ca.digilogue.xp.App;
import ca.digilogue.xp.generator.OhlcvCandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Kafka consumer service for consuming OHLCV candles collection from Kafka.
 * Updates the static latestCandles map in App.java with the consumed data.
 */
@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    /**
     * Consumes OHLCV candles collection from Kafka topic.
     * Updates the static latestCandles map in App.java.
     * 
     * @param candles Map of symbol to OHLCV candle (the entire collection)
     * @param acknowledgment Kafka acknowledgment for manual commit (if needed)
     */
    @KafkaListener(topics = "${spring.kafka.topic.ohlcv:ohlcv-topic}", groupId = "${spring.kafka.consumer.group-id:xp-marketdata-service-group}")
    public void consumeCandlesCollection(
            @Payload Map<String, OhlcvCandle> candles,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {
        
        try {
            if (candles == null || candles.isEmpty()) {
                log.debug("Received empty candles collection from topic: {}, partition: {}, offset: {}", 
                    topic, partition, offset);
                return;
            }

            // Update the static latestCandles map in App.java
            synchronized (App.latestCandles) {
                App.latestCandles.clear();
                App.latestCandles.putAll(candles);
            }

            log.debug("Consumed and updated {} candles from topic: {}, partition: {}, offset: {}", 
                candles.size(), topic, partition, offset);

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
