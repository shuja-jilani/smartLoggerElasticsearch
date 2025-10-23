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


    public KafkaConsumerService(TransformDataService transformDataService, ElasticsearchService elasticsearchService, KafkaProducerService kafkaProducerService) {
        this.transformDataService = transformDataService;
        this.elasticsearchService = elasticsearchService;
        this.kafkaProducerService = kafkaProducerService;
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
                JsonNode rawData = messageNode.get("source");
                System.out.println("Received from Kafka: " + rawData);
                transformedData = this.transformDataService.transformData(rawData, connection);

            boolean isIndexed = false;
            if (transformedData != null) {
                isIndexed = this.elasticsearchService.indexToElasticsearch(transformedData);
            }

            if (isIndexed) {
                System.out.println("Data was indexed SUCCESSFULLY");
            } else {
                System.out.println("Data was NOT indexed successfully");
            }
        } catch (Exception e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
        }

    }

    @KafkaListener(topics = {"connection-topic_es"}, groupId = "connection-group")
    public void consumeConnectionData(String message) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode messageNode = objectMapper.readTree(message);
            Connection connection = objectMapper.treeToValue(messageNode.get("connection"), Connection.class);

            String connectionType = connection.getConnectiontype();

            JsonNode rawData = elasticsearchService.fetchData(connection);

            if (rawData == null || rawData.isMissingNode()) {
                System.out.println("DEBUG: No raw data returned from Elasticsearch for the given timeframe.");
                return;
            }

            JsonNode hitsArray = rawData.path("hits").path("hits");
            if (!hitsArray.isEmpty() && hitsArray.isArray()) {
                for (JsonNode hit : hitsArray) {
                    JsonNode source = hit.get("_source");
                    ObjectNode enrichedMessage = objectMapper.createObjectNode();
                    enrichedMessage.putPOJO("connection", connection);
                    enrichedMessage.set("source", source);
                    System.out.println("DEBUG: Sending to Kafka -> " + enrichedMessage);
                    kafkaProducerService.sendRawData(enrichedMessage);
                }
            } else {
                System.out.println("DEBUG: No hits found in Elasticsearch response for the given timeframe.");
            }

        } catch (Exception e) {
            System.err.println("Error processing connection message: " + e.getMessage());
            e.printStackTrace();
        }
    }

}