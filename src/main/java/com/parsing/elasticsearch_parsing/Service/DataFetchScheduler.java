package com.parsing.elasticsearch_parsing.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class DataFetchScheduler {
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZone(ZoneId.of("Asia/Riyadh"));

    public DataFetchScheduler(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

//    @Scheduled(fixedRate = 120000) // Runs every 2 minutes
    public void publishConnectionsAutomatically() {
        Instant now = Instant.now();
        Instant twoMinutesAgo = now.minusSeconds(120L);
        String gte = FORMATTER.format(twoMinutesAgo);
        String lte = FORMATTER.format(now);
        String esUrl = "http://localhost:8083/fetch-and-publish?gte=" + gte + "&lte=" + lte;

        try {
            String esResponse = (String)this.restTemplate.getForObject(esUrl, String.class, new Object[0]);
            if (esResponse != null) {
                JsonNode jsonNode = this.objectMapper.readTree(esResponse);
                String message = jsonNode.has("message") ? jsonNode.get("message").asText() : "No ES message.";
                System.out.println("Auto Publish (ES): " + message + " | URL: " + esUrl);
            }
        } catch (Exception e) {
            System.err.println("Error during scheduled publish: " + e.getMessage());
            e.printStackTrace();
        }

    }
}
