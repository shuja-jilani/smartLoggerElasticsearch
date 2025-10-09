package com.parsing.elasticsearch_parsing.Service.kafka;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.parsing.elasticsearch_parsing.Entity.Connection;
import com.parsing.elasticsearch_parsing.Service.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    private final TransformDataService transformDataService;
    private final ElasticsearchService elasticsearchService;
    private final KafkaProducerService kafkaProducerService;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final DatabaseService databaseService;

    public KafkaConsumerService(TransformDataService transformDataService, ElasticsearchService elasticsearchService, KafkaProducerService kafkaProducerService, DatabaseService databaseService) {
        this.transformDataService = transformDataService;
        this.elasticsearchService = elasticsearchService;
        this.kafkaProducerService = kafkaProducerService;
        this.databaseService = databaseService;
    }

    @KafkaListener(
            topics = {"raw-data-topic_es"},
            groupId = "transform-group"
    )
    public void consumeRawData(String message) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode messageNode = objectMapper.readTree(message);
            Connection connection = (Connection)objectMapper.treeToValue(messageNode.get("connection"), Connection.class);
            String connectionType = connection.getConnectiontype();
            System.out.println("DEBUG: Connection Type = " + connectionType);
            Map<String, Object> transformedData = null;
            if ("elasticsearch".equalsIgnoreCase(connectionType)) {
                JsonNode rawData = messageNode.get("source");
                System.out.println("Received from Kafka: " + rawData);
                transformedData = this.transformDataService.transformData(rawData, connection);
            } else if ("database".equalsIgnoreCase(connectionType)) {
                JsonNode rawData = messageNode.get("source");
                System.out.println("Received from Kafka (DB): " + rawData);
                transformedData = this.transformDataService.transformDBData(rawData, connection);
            }

            boolean isIndexed = false;
            if (transformedData != null) {
                isIndexed = this.elasticsearchService.indexToElasticsearch(transformedData);
            }

            if (isIndexed) {
                this.processedCount.incrementAndGet();
                System.out.println("Data was indexed successfully, count increment.");
            } else {
                System.out.println("Data was not indexed successfully. Skipping count increment.");
            }
        } catch (Exception e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
        }

    }

    public int getProcessedCount() {
        return this.processedCount.get();
    }

    @KafkaListener(
            topics = {"connection-topic_es"},
            groupId = "connection-group"
    )
    public void consumeConnectionData(String message) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode messageNode = objectMapper.readTree(message);
            Connection connection = (Connection)objectMapper.treeToValue(messageNode.get("connection"), Connection.class);
            String gte = messageNode.get("gte").asText();
            String lte = messageNode.get("lte").asText();
            String connectionType = connection.getConnectiontype();
            System.out.println("DEBUG: Connection Type = " + connectionType);
            if ("elasticsearch".equalsIgnoreCase(connectionType)) {
                JsonNode rawData = this.elasticsearchService.fetchData(connection, gte, lte);
                JsonNode hitsArray = rawData.path("hits").path("hits");
                if (hitsArray != null && hitsArray.isArray()) {
                    for(JsonNode hit : hitsArray) {
                        JsonNode source = hit.get("_source");
                        ObjectNode enrichedMessage = objectMapper.createObjectNode();
                        enrichedMessage.putPOJO("connection", connection);
                        enrichedMessage.set("source", source);
                        System.out.println("DEBUG: Sending to Kafka -> " + enrichedMessage);
                        this.kafkaProducerService.sendRawData(enrichedMessage);
                    }
                } else {
                    System.out.println("DEBUG: No hits found in Elasticsearch response.");
                }
            } else if ("database".equalsIgnoreCase(connectionType)) {
                try {
                    List<Map<String, Object>> records = this.databaseService.fetchTableData(connection, gte, lte);

                    for(Map<String, Object> row : records) {
                        ObjectNode enrichedMessage = objectMapper.createObjectNode();
                        Map<String, Object> normalizedRow = new HashMap();

                        for(Map.Entry<String, Object> entry : row.entrySet()) {
                            Object value = entry.getValue();
                            if (value instanceof Timestamp) {
                                String formatted = value.toString();
                                normalizedRow.put((String)entry.getKey(), formatted);
                            } else {
                                normalizedRow.put((String)entry.getKey(), value);
                            }
                        }

                        enrichedMessage.putPOJO("connection", connection);
                        enrichedMessage.set("source", objectMapper.valueToTree(normalizedRow));
                        System.out.println("DEBUG: Sending DB record to Kafka -> " + enrichedMessage);
                        this.kafkaProducerService.sendRawData(enrichedMessage);
                    }

                    if (records.isEmpty()) {
                        System.out.println("DEBUG: No records found in DB table for given time range.");
                    }
                } catch (Exception e) {
                    System.err.println("ERROR: Failed to fetch or send DB records: " + e.getMessage());
                    e.printStackTrace();
                }
            } else {
                System.out.println("WARN: Unsupported connection type: " + connectionType);
            }
        } catch (Exception e) {
            System.err.println("Error processing connection message: " + e.getMessage());
            e.printStackTrace();
        }

    }
}