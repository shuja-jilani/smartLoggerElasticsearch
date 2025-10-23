package com.parsing.elasticsearch_parsing.Controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.*;

import com.parsing.elasticsearch_parsing.Entity.Connection;
import com.parsing.elasticsearch_parsing.Repository.ConnectionRepository;
import com.parsing.elasticsearch_parsing.Service.ConnectionPublisherService;
import com.parsing.elasticsearch_parsing.Service.DatabaseService;
import com.parsing.elasticsearch_parsing.Service.ElasticsearchService;
import com.parsing.elasticsearch_parsing.Service.TransformDataService;
import com.parsing.elasticsearch_parsing.Service.kafka.KafkaConsumerService;
import com.parsing.elasticsearch_parsing.Service.kafka.KafkaProducerService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping({"/api/data"})
public class DataController {


    private final ConnectionPublisherService connectionPublisherService;

    public DataController(ConnectionPublisherService connectionPublisherService) {
        this.connectionPublisherService = connectionPublisherService;
    }

    @GetMapping("/fetch-and-publish")
    public Map<String, Object> fetchAndPublish() {
        String message = connectionPublisherService.publishConnectionsToKafka();
        return Collections.singletonMap("message", message);
    }

}