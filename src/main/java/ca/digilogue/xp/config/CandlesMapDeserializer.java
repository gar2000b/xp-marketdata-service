package ca.digilogue.xp.config;

import ca.digilogue.xp.generator.OhlcvCandle;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Custom deserializer for Map<String, OhlcvCandle> from Kafka messages.
 * Properly deserializes the JSON Map structure using Jackson with TypeReference.
 */
public class CandlesMapDeserializer implements Deserializer<Map<String, OhlcvCandle>> {

    private static final Logger log = LoggerFactory.getLogger(CandlesMapDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, OhlcvCandle> deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return objectMapper.readValue(data, new TypeReference<Map<String, OhlcvCandle>>() {});
        } catch (IOException e) {
            log.error("Error deserializing candles map from topic: {}", topic, e);
            throw new RuntimeException("Failed to deserialize candles map", e);
        }
    }
}
