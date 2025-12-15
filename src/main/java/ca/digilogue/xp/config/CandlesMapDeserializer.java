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
            log.warn("Received null data from topic: {}", topic);
            return null;
        }

        log.debug("Deserializing message from topic: {}, data length: {} bytes", topic, data.length);
        
        try {
            String jsonString = new String(data);
            log.debug("Raw JSON data (first 200 chars): {}", jsonString.length() > 200 ? jsonString.substring(0, 200) : jsonString);
            
            Map<String, OhlcvCandle> result = objectMapper.readValue(data, new TypeReference<Map<String, OhlcvCandle>>() {});
            log.debug("Successfully deserialized {} candles from topic: {}", result != null ? result.size() : 0, topic);
            return result;
        } catch (IOException e) {
            log.error("ERROR deserializing candles map from topic: {}. Data length: {} bytes. Error: {}", 
                    topic, data != null ? data.length : 0, e.getMessage(), e);
            // Log first 500 chars of the data for debugging
            if (data != null && data.length > 0) {
                try {
                    String dataPreview = new String(data);
                    log.error("Data preview (first 500 chars): {}", 
                            dataPreview.length() > 500 ? dataPreview.substring(0, 500) : dataPreview);
                } catch (Exception ex) {
                    log.error("Could not convert data to string for preview", ex);
                }
            }
            throw new RuntimeException("Failed to deserialize candles map", e);
        }
    }
}
