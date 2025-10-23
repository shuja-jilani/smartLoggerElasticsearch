package com.parsing.elasticsearch_parsing.Controller;


import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.parsing.elasticsearch_parsing.Entity.Connection;
import com.parsing.elasticsearch_parsing.Repository.ConnectionRepository;
import com.parsing.elasticsearch_parsing.Service.DatabaseService;
import com.parsing.elasticsearch_parsing.Service.ElasticsearchService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping({"/api/data"})
public class ElasticsearchController {
    private final ElasticsearchService elasticsearchService;
    private final ConnectionRepository connectionRepository;

    public ElasticsearchController(ElasticsearchService elasticsearchService, DatabaseService databaseService, ConnectionRepository connectionRepository) {
        this.elasticsearchService = elasticsearchService;
        this.connectionRepository = connectionRepository;
    }
//
//    @GetMapping({"/fetch"})
//    public ResponseEntity<Map<String, JsonNode>> fetchData(@RequestParam String gte, @RequestParam String lte) throws IOException {
//        List<Connection> connections = this.connectionRepository.findByConnectionType("elasticsearch");
//        Map<String, JsonNode> responseMap = new HashMap();
//
//        for(Connection connection : connections) {
//            JsonNode rawData = this.elasticsearchService.fetchData(connection, gte, lte);
//            responseMap.put(connection.getConnectionName(), rawData);
//        }
//
//        return ResponseEntity.ok(responseMap);
//    }
}