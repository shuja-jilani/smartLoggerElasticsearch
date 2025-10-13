package com.parsing.elasticsearch_parsing.Service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.parsing.elasticsearch_parsing.Entity.Connection;
import com.parsing.elasticsearch_parsing.Repository.ApiMetadataRepository;
import com.parsing.elasticsearch_parsing.Repository.ConnectionRepository;
import lombok.Data;
import org.springframework.http.*;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.Map;

@Service
@Data
public class ElasticsearchService {
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final ConnectionRepository connectionRepository;
    private ApiMetadataRepository apiMetadataRepository;

    public JsonNode fetchData(Connection connection, String gte, String lte) throws IOException {
        JsonNode details = this.objectMapper.readTree(connection.getDetails());
        String clusterUrl = details.get("clusterURL").asText();
        String dataset = details.get("dataset").asText();
        String timeField = "RequestTime";
        String resourcePathField = "ResourcePath";

        for(JsonNode field : details.get("fields")) {
            if ("RequestTime".equals(field.get("field").asText())) {
                timeField = field.get("identifier").asText();
            }
        }

        String queryJson = "{\n  \"size\": 1000,\n  \"query\": {\n    \"bool\": {\n      \"must\": [\n        {\n          \"range\": {\n            \"" + timeField + "\": {\n              \"gte\": \"" + gte + "\",\n              \"lte\": \"" + lte + "\"\n            }\n          }\n        }\n      ]\n    }\n  }\n}";
        String url = clusterUrl + "/" + dataset + "/_search";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity(queryJson, headers);
        ResponseEntity<String> response = this.restTemplate.postForEntity(url, request, String.class, new Object[0]);
        return this.objectMapper.readTree((String)response.getBody());
    }

    public boolean indexToElasticsearch(Map<String, Object> data) {
        try {
            String esUrl = "http://elasticsearch:9200/my_smartlogger_index2/_doc";
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> request = new HttpEntity(this.objectMapper.writeValueAsString(data), headers);
            ResponseEntity<String> response = this.restTemplate.postForEntity(esUrl, request, String.class, new Object[0]);
            return response.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            System.err.println("Error indexing document: " + e.getMessage());
            return false;
        }
    }
}