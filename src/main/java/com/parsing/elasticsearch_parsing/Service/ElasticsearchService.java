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
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Service
public class ElasticsearchService {
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public ElasticsearchService(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    public JsonNode fetchData(Connection connection) throws IOException {
        JsonNode details = objectMapper.readTree(connection.getDetails());
        String clusterUrl = details.get("clusterURL").asText();
        String dataset = details.get("dataset").asText();

        // Default time field info
        String timeFieldIdentifier = "RequestTime";
        String dateType = "date"; // default
        String datePattern = "yyyy-MM-dd'T'HH:mm:ss.SSS";
        ZoneId zoneId = ZoneId.of("UTC");

        // Extract RequestTime metadata
        for (JsonNode field : details.get("fields")) {
            if ("RequestTime".equals(field.get("field").asText())) {
                timeFieldIdentifier = field.get("identifier").asText();
                if (field.hasNonNull("dateType")) {
                    dateType = field.get("dateType").asText();
                }
                if (field.hasNonNull("datePattern") && !field.get("datePattern").asText().isEmpty()) {
                    datePattern = field.get("datePattern").asText();
                }
                break;
            }
        }

        String gteStr;
        String lteStr;

        // Generate gte/lte depending on dateType
        if ("epoch".equalsIgnoreCase(dateType)) {
            long lte = System.currentTimeMillis();           // current epoch millis
            long gte = lte - (2 * 60 * 1000);                // minus 2 minutes
            gteStr = String.valueOf(gte);
            lteStr = String.valueOf(lte);
//            gteStr = String.valueOf(1758781560985L);
//            lteStr = String.valueOf(1758781561183L);
        } else {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(datePattern).withZone(zoneId);
            Instant now = Instant.now();
            Instant twoMinutesAgo = now.minusSeconds(120L);
            gteStr = formatter.format(twoMinutesAgo);
            lteStr = formatter.format(now);
        }

        // Build Elasticsearch range query dynamically
        String queryJson = "{\n" +
                "  \"size\": 1000,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"range\": {\n" +
                "            \"" + timeFieldIdentifier + "\": {\n" +
                "              \"gte\": \"" + gteStr + "\",\n" +
                "              \"lte\": \"" + lteStr + "\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        String url = clusterUrl + "/" + dataset + "/_search";
        System.out.println("URL | " + url + " | \n" + queryJson);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(queryJson, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
        return objectMapper.readTree(response.getBody());
    }


    public boolean indexToElasticsearch(Map<String, Object> data) {
        try {
            String esUrl = "http://localhost:9200/my_smartlogger_index2/_doc";
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