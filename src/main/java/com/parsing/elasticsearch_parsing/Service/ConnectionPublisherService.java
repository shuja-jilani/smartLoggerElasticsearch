package com.parsing.elasticsearch_parsing.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.parsing.elasticsearch_parsing.Entity.Connection;
import com.parsing.elasticsearch_parsing.Repository.ConnectionRepository;
import com.parsing.elasticsearch_parsing.Service.kafka.KafkaProducerService;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ConnectionPublisherService {

    private final ConnectionRepository connectionRepository;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;

    public ConnectionPublisherService(ConnectionRepository connectionRepository,
                                      KafkaProducerService kafkaProducerService,
                                      ObjectMapper objectMapper) {
        this.connectionRepository = connectionRepository;
        this.kafkaProducerService = kafkaProducerService;
        this.objectMapper = objectMapper;
    }

    public String publishConnectionsToKafka() {
        List<Connection> connections = connectionRepository.findByConnectionType("elasticsearch");

        if (connections.isEmpty()) {
            return "No Elasticsearch connections found.";
        }

        int publishedCount = 0;

        for (Connection connection : connections) {
            if ("ENABLED".equalsIgnoreCase(connection.getStatus())) {
                ObjectNode message = objectMapper.createObjectNode();
                message.putPOJO("connection", connection);
                kafkaProducerService.sendConnectionData(message);
                publishedCount++;
            }
        }

        if (publishedCount == 0) {
            return "No ENABLED Elasticsearch connections found.";
        }

        return publishedCount + " ENABLED connections published to Kafka.";
    }
}
