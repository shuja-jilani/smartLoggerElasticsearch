package com.parsing.elasticsearch_parsing.Controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.*;

import com.parsing.elasticsearch_parsing.Entity.Connection;
import com.parsing.elasticsearch_parsing.Repository.ConnectionRepository;
import com.parsing.elasticsearch_parsing.Service.DatabaseService;
import com.parsing.elasticsearch_parsing.Service.ElasticsearchService;
import com.parsing.elasticsearch_parsing.Service.TransformDataService;
import com.parsing.elasticsearch_parsing.Service.kafka.KafkaConsumerService;
import com.parsing.elasticsearch_parsing.Service.kafka.KafkaProducerService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping({"/api/data"})
public class DataController {
    private final KafkaProducerService kafkaProducerService;
    private final KafkaConsumerService kafkaConsumerService;
    private final ConnectionRepository connectionRepository;
    private final ElasticsearchService elasticsearchService;
    private final TransformDataService transformDataService;
    private final DatabaseService databaseService;

    public DataController(KafkaProducerService kafkaProducerService, KafkaConsumerService kafkaConsumerService, ConnectionRepository connectionRepository, ElasticsearchService elasticsearchService, TransformDataService transformDataService, DatabaseService databaseService) {
        this.kafkaProducerService = kafkaProducerService;
        this.kafkaConsumerService = kafkaConsumerService;
        this.connectionRepository = connectionRepository;
        this.elasticsearchService = elasticsearchService;
        this.transformDataService = transformDataService;
        this.databaseService = databaseService;
    }

    @GetMapping({"/fetch-and-publish"})
    public Map<String, Object> fetchAndPublish(@RequestParam String gte, @RequestParam String lte) {
        List<Connection> connections = this.connectionRepository.findByConnectionType("elasticsearch");
        if (connections.isEmpty()) {
            return Collections.singletonMap("message", "No Elasticsearch connections found.");
        } else {
            for(Connection connection : connections) {
                ObjectNode message = (new ObjectMapper()).createObjectNode();
                message.putPOJO("connection", connection);
                message.put("gte", gte);
                message.put("lte", lte);
                this.kafkaProducerService.sendConnectionData(message);
            }

            return Collections.singletonMap("message", "Connections published to Kafka.");
        }
    }

    @GetMapping({"/fetch-and-publish-db"})
    public Map<String, Object> fetchAndPublishForDB(@RequestParam String gte, @RequestParam String lte) {
        List<Connection> connections = this.connectionRepository.findByConnectionType("database");
        if (connections.isEmpty()) {
            return Collections.singletonMap("message", "No Elasticsearch connections found.");
        } else {
            for(Connection connection : connections) {
                ObjectNode message = (new ObjectMapper()).createObjectNode();
                message.putPOJO("connection", connection);
                message.put("gte", gte);
                message.put("lte", lte);
                this.kafkaProducerService.sendConnectionData(message);
            }

            return Collections.singletonMap("message", "Connections published to Kafka.");
        }
    }

    @GetMapping({"/processed-count"})
    public Map<String, Object> getProcessedCount() {
        Map<String, Object> response = new HashMap();
        response.put("processedCount", this.kafkaConsumerService.getProcessedCount());
        return response;
    }
}