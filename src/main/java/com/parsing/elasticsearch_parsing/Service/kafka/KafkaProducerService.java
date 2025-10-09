package com.parsing.elasticsearch_parsing.Service.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String RAW_DATA_TOPIC_ES = "raw-data-topic_es";
    private static final String CONNECTION_TOPIC_ES = "connection-topic_es";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendRawData(JsonNode rawData) {
        try {
            String jsonString = this.objectMapper.writeValueAsString(rawData);
            this.kafkaTemplate.send("raw-data-topic_es", jsonString);
            System.out.println("Sent to Kafka (Raw Data): " + jsonString);
        } catch (Exception e) {
            System.err.println("Error serializing raw data JSON: " + e.getMessage());
        }

    }

    public void sendConnectionData(JsonNode connectionData) {
        try {
            String jsonString = this.objectMapper.writeValueAsString(connectionData);
            this.kafkaTemplate.send("connection-topic_es", jsonString);
            System.out.println("Sent to Kafka (Connection Data): " + jsonString);
        } catch (Exception e) {
            System.err.println("Error serializing connection data JSON: " + e.getMessage());
        }

    }
}