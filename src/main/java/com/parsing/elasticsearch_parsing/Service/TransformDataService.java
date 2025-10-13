package com.parsing.elasticsearch_parsing.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.text.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.parsing.elasticsearch_parsing.Entity.*;
import com.parsing.elasticsearch_parsing.Repository.ApiMetadataFieldRepository;
import com.parsing.elasticsearch_parsing.Repository.ApiMetadataRepository;
import lombok.Data;
import org.springframework.stereotype.Service;

@Service
@Data
public class TransformDataService {
    private final DatabaseService databaseService;
    private final ApiMetadataRepository apiMetadataRepository;
    private final ObjectMapper objectMapper;
    private final ApiMetadataFieldRepository apiMetadataFieldRepository;
    private final AutoDiscovery autoDiscovery;

    public Map<String, Object> transformData(JsonNode source, Connection connection) throws JsonProcessingException {
        Map<String, Object> transformedData = new HashMap();
        String contentType = "date";
        String requestPattern = "yyyy-MM-dd'T'HH:mm:ss.SSS";
        JsonNode details = this.objectMapper.readTree(connection.getDetails());
        String pathToResourcePath = "ResourcePath";

        for(JsonNode field : details.get("fields")) {
            if ("ResourcePath".equals(field.get("field").asText())) {
                pathToResourcePath = field.get("path").asText();
            }

            if ("RequestTime".equals(field.get("field").asText())) {
                contentType = field.get("contentType").asText();
            }
        }

        if ("date".equalsIgnoreCase(contentType) && details.has("patterns") && details.get("patterns").has("RequestTime")) {
            requestPattern = details.get("patterns").get("RequestTime").asText();
        }

        Optional<JsonNode> resourcePathNode = this.getValueFromPath(source, pathToResourcePath);
        if (!resourcePathNode.isPresent()) {
            return null;
        } else {
            String resourcePath = ((JsonNode)resourcePathNode.get()).asText();
            ApiMetadata matchingApi = this.apiMetadataRepository.findByResourcePath(resourcePath);
            if (matchingApi == null) {
                this.autoDiscovery.performAutoDiscovery(connection, resourcePath);
                return null;
            } else if (Objects.equals(matchingApi.getStatus(), "disabled")) {
                return null;
            } else {
                List<ApiMetadataField> fields = apiMetadataFieldRepository.findByApiMetadataId(matchingApi.getUniqueId());
                List<Map<String, String>> customFields = new ArrayList<>();

                for (ApiMetadataField field : fields) {
                    String path = field.getPath();          // use path for locating data in JSON
                    String fieldName = field.getField();    // output field name
                    String keyStatus = field.getKey_status();
                    String fieldContentType = field.getContentType();

                    Optional<JsonNode> valueNode = Optional.empty();

                    // --- Handle RequestPayload separately ---
                    if ("RequestPayload".equalsIgnoreCase(fieldName)) {
                        valueNode = getValueFromPath(source, path);
                        if (valueNode.isPresent()) {
                            // payload is a stringified JSON → take as text
                            transformedData.put(fieldName, valueNode.get().asText());
                        }
                    }
                    // --- Handle ResponsePayload separately ---
                    else if ("ResponsePayload".equalsIgnoreCase(fieldName)) {
                        valueNode = getValueFromPath(source, path);
                        if (valueNode.isPresent()) {
                            transformedData.put(fieldName, valueNode.get().asText());
                        }
                    }
                    // --- Handle all other fields normally ---
                    else {
                        valueNode = getValueFromPath(source, path);
                        if (valueNode.isPresent()) {
                            String value = valueNode.get().asText();

                            if ("Custom".equalsIgnoreCase(keyStatus)) {
                                Map<String, String> customFieldEntry = new HashMap<>();
                                customFieldEntry.put("key", fieldName);
                                customFieldEntry.put("value", value);
                                customFields.add(customFieldEntry);
                            } else {
                                transformedData.put(fieldName, value);
                            }
                        }
                    }
                }

                if (!customFields.isEmpty()) {
                    transformedData.put("customFields", objectMapper.valueToTree(customFields));
                }


                if (transformedData.containsKey("RequestTime") && transformedData.containsKey("ResponseTime")) {
                    try {
                        if ("date".equalsIgnoreCase(contentType)) {
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(requestPattern);
                            LocalDateTime requestDateTime = LocalDateTime.parse(transformedData.get("RequestTime").toString(), formatter);
                            LocalDateTime responseDateTime = LocalDateTime.parse(transformedData.get("ResponseTime").toString(), formatter);
                            long elapsed = Duration.between(requestDateTime, responseDateTime).toMillis();
                            transformedData.put("ElapsedTime", elapsed);
                        } else if ("epoch".equalsIgnoreCase(contentType)) {
                            long requestEpoch = Long.parseLong(transformedData.get("RequestTime").toString());
                            long responseEpoch = Long.parseLong(transformedData.get("ResponseTime").toString());
                            long elapsed = responseEpoch - requestEpoch;
                            transformedData.put("ElapsedTime", elapsed);
                            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                            transformedData.put("RequestTime", dateFormat.format(new Date(requestEpoch)));
                            transformedData.put("ResponseTime", dateFormat.format(new Date(responseEpoch)));
                        }
                    } catch (Exception e) {
                        transformedData.put("ElapsedTime", "Invalid Time Format: " + e.getMessage());
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
