package com.parsing.elasticsearch_parsing.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;

import com.parsing.elasticsearch_parsing.Entity.Connection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class AutoDiscovery {
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final DatabaseService databaseService;

    public AutoDiscovery(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, DatabaseService databaseService) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.databaseService = databaseService;
    }

    public void performAutoDiscovery(Connection connection, String resourcePath) {
        try {
            String connectionName = connection.getConnectionName();
            String connectionType = connection.getConnectiontype();
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode details = objectMapper.readTree(connection.getDetails());
            String dataset = details.get("dataset").asText();

            UUID uniqueId = UUID.randomUUID();
            String roleNames = "Admin";
            String status = "disabled";
            this.databaseService.insertApiMetadata(uniqueId, connectionName, dataset, resourcePath, roleNames, status, resourcePath);
            JsonNode fields = details.get("fields");
            if (fields != null && fields.isArray()) {
                for(JsonNode fieldNode : fields) {
                    this.databaseService.insertApiMetadataField(uniqueId, fieldNode.get("field").asText(), fieldNode.get("identifier").asText(), fieldNode.get("datatype").asText(), fieldNode.get("contentType").asText(), fieldNode.get("keyStatus").asText(), fieldNode.get("path").asText());
                }
            }

            System.out.println("New API discovered and added to database: " + resourcePath);
        } catch (Exception e) {
            System.err.println("Error during API auto-discovery: " + e.getMessage());
            e.printStackTrace();
        }

    }
}
