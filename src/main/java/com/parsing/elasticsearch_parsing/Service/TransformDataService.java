package com.parsing.elasticsearch_parsing.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.text.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.parsing.elasticsearch_parsing.Entity.*;
import com.parsing.elasticsearch_parsing.Repository.ApiMetadataFieldRepository;
import com.parsing.elasticsearch_parsing.Repository.ApiMetadataRepository;
import com.parsing.elasticsearch_parsing.Service.utils.FieldExtractorUtil;
import lombok.Data;
import org.springframework.stereotype.Service;

@Service
public class TransformDataService {
    private final DatabaseService databaseService;
    private final ApiMetadataRepository apiMetadataRepository;
    private final ObjectMapper objectMapper;
    private final ApiMetadataFieldRepository apiMetadataFieldRepository;
    private final AutoDiscovery autoDiscovery;

    public TransformDataService(DatabaseService databaseService, ApiMetadataRepository apiMetadataRepository, ObjectMapper objectMapper, ApiMetadataFieldRepository apiMetadataFieldRepository, AutoDiscovery autoDiscovery) {
        this.databaseService = databaseService;
        this.apiMetadataRepository = apiMetadataRepository;
        this.objectMapper = objectMapper;
        this.apiMetadataFieldRepository = apiMetadataFieldRepository;
        this.autoDiscovery = autoDiscovery;
    }


    public Map<String, Object> transformData(JsonNode source, Connection connection) throws JsonProcessingException {
        Map<String, Object> transformedData = new HashMap();
        JsonNode details = this.objectMapper.readTree(connection.getDetails());
        String pathToResourcePath = "ResourcePath";

        for (JsonNode field : details.get("fields")) {
            String fieldName = field.get("field").asText();

            if ("ResourcePath".equals(fieldName)) {
                pathToResourcePath = field.get("path").asText();
            }
        }
        Optional<JsonNode> resourcePathNode = this.getValueFromPath(source, pathToResourcePath);
        if (!resourcePathNode.isPresent()) {
            return null;
        } else {
            String resourcePath = ((JsonNode)resourcePathNode.get()).asText();
            ApiMetadata matchingApi = this.apiMetadataRepository.findByConnectionNameAndResourcePath(connection.getConnectionName(), resourcePath);
            if (matchingApi == null) {
                this.autoDiscovery.performAutoDiscovery(connection, resourcePath);
                return null;
            } else if (Objects.equals(matchingApi.getStatus(), "disabled")) {
                return null;
            } else {
                List<ApiMetadataField> fields = apiMetadataFieldRepository.findByApiMetadataId(matchingApi.getUniqueId());
                List<Map<String, String>> customFields = new ArrayList<>();

                for (ApiMetadataField field : fields) {
                    String fieldName = field.getField();
                    String keyStatus = field.getKey_status();

                    String extractedValue = FieldExtractorUtil.extractValue(source, field);

                    if (extractedValue != null) {
                        if ("Custom".equalsIgnoreCase(keyStatus)) {
                            Map<String, String> customFieldEntry = new HashMap<>();
                            customFieldEntry.put("key", fieldName);
                            customFieldEntry.put("value", extractedValue);
                            customFields.add(customFieldEntry);
                        } else {
                            transformedData.put(fieldName, extractedValue);
                        }
                    }
                }

                if (!customFields.isEmpty()) {
                    transformedData.put("CustomField", objectMapper.valueToTree(customFields));
                }

                if (transformedData.containsKey("RequestTime") && transformedData.containsKey("ResponseTime")) {
                    try {
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
                        LocalDateTime requestDateTime = LocalDateTime.parse(transformedData.get("RequestTime").toString(), formatter);
                        LocalDateTime responseDateTime = LocalDateTime.parse(transformedData.get("ResponseTime").toString(), formatter);
                        long elapsed = Duration.between(requestDateTime, responseDateTime).toMillis();
                        transformedData.put("ElapsedTime", elapsed);
                    } catch (Exception e) {
                        transformedData.put("ElapsedTime", 0L);
                    }
                }

                transformedData.put("APIName", matchingApi.getApi_name());
                transformedData.put("ResourcePath", matchingApi.getResourcePath());
                String roleNames = matchingApi.getRoleNames();
                if (roleNames != null && !roleNames.isEmpty()) {
                    List<String> roles = Arrays.asList(roleNames.split(","));
                    ArrayNode rolesArrayNode = this.objectMapper.createArrayNode();
                    roles.forEach((role) -> rolesArrayNode.add(role.trim()));
                    transformedData.put("Role", rolesArrayNode);
                }

                return transformedData;
            }
        }
    }

    private Optional<JsonNode> getValueFromPath(JsonNode node, String path) {
        String[] keys = path.split("\\.");
        JsonNode current = node;

        for(String key : keys) {
            if (current == null || !current.has(key)) {
                return Optional.empty();
            }
            current = current.get(key);
        }
        return Optional.ofNullable(current);
    }
}
