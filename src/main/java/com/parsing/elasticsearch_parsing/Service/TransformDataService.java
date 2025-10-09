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
                List<ApiMetadataField> fields = this.apiMetadataFieldRepository.findByApiMetadataId(matchingApi.getUniqueId());
                Optional<JsonNode> requestNode = this.getValueFromPath(source, "request");
                if (requestNode.isPresent()) {
                    JsonNode request = (JsonNode)requestNode.get();
                    ObjectNode filteredRequest = this.objectMapper.createObjectNode();
                    if (request.has("Headers")) {
                        filteredRequest.set("headers", request.get("Headers"));
                    }

                    if (request.has("payload")) {
                        filteredRequest.set("payload", request.get("payload"));
                    }

                    transformedData.put("request", filteredRequest);
                }

                Optional<JsonNode> responseNode = this.getValueFromPath(source, "response");
                if (responseNode.isPresent()) {
                    JsonNode response = (JsonNode)responseNode.get();
                    ObjectNode filteredResponse = this.objectMapper.createObjectNode();
                    if (response.has("Headers")) {
                        filteredResponse.set("headers", response.get("Headers"));
                    }

                    if (response.has("payload")) {
                        filteredResponse.set("payload", response.get("payload"));
                    }

                    transformedData.put("response", filteredResponse);
                }

                List<Map<String, String>> customFields = new ArrayList();

                for(ApiMetadataField field : fields) {
                    String path = field.getPath();
                    String fieldName = field.getField();
                    String keyStatus = field.getKey_status();
                    Optional<JsonNode> valueNode;
                    if (path.contains(".payload.")) {
                        Optional<JsonNode> payloadNode = this.getValueFromPath(source, "request.payload");
                        if (payloadNode.isPresent()) {
                            try {
                                JsonNode payloadJson = this.objectMapper.readTree(((JsonNode)payloadNode.get()).asText());
                                valueNode = this.getValueFromPath(payloadJson, path.replace("request.payload.", ""));
                            } catch (Exception var24) {
                                valueNode = Optional.empty();
                            }
                        } else {
                            valueNode = Optional.empty();
                        }
                    } else {
                        valueNode = this.getValueFromPath(source, path);
                    }

                    if (valueNode.isPresent()) {
                        String value = ((JsonNode)valueNode.get()).asText();
                        if ("Custom".equalsIgnoreCase(keyStatus)) {
                            Map<String, String> customFieldEntry = new HashMap();
                            customFieldEntry.put("key", fieldName);
                            customFieldEntry.put("value", value);
                            customFields.add(customFieldEntry);
                        } else {
                            transformedData.put(fieldName, value);
                        }
                    }
                }

                if (!customFields.isEmpty()) {
                    transformedData.put("CustomField", customFields);
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

    public Map<String, Object> transformDBData(JsonNode source, Connection connection) throws JsonProcessingException {
        Map<String, Object> transformedData = new HashMap();
        String contentType = "date";
        String requestPattern = "yyyy-MM-dd'T'HH:mm:ss.SSS";
        JsonNode details = this.objectMapper.readTree(connection.getDetails());
        String resourcePathIdentifier = "ResourcePath";

        for(JsonNode field : details.get("fields")) {
            if ("ResourcePath".equals(field.get("field").asText())) {
                resourcePathIdentifier = field.get("identifier").asText();
            }

            if ("RequestTime".equals(field.get("field").asText())) {
                contentType = field.get("contentType").asText();
            }
        }

        if ("date".equalsIgnoreCase(contentType) && details.has("patterns") && details.get("patterns").has("RequestTime")) {
            requestPattern = details.get("patterns").get("RequestTime").asText();
        }

        JsonNode resourcePathNode = source.get(resourcePathIdentifier);
        if (resourcePathNode != null && !resourcePathNode.isNull()) {
            String resourcePath = resourcePathNode.asText();
            ApiMetadata matchingApi = this.apiMetadataRepository.findByResourcePath(resourcePath);
            if (matchingApi == null) {
                this.autoDiscovery.performAutoDiscovery(connection, resourcePath);
                return null;
            } else if ("disabled".equalsIgnoreCase(matchingApi.getStatus())) {
                return null;
            } else {
                List<ApiMetadataField> fields = this.apiMetadataFieldRepository.findByApiMetadataId(matchingApi.getUniqueId());
                ObjectNode requestBlock = this.objectMapper.createObjectNode();
                if (source.has("RequestHeaders")) {
                    JsonNode requestHeaders = this.objectMapper.readTree(source.get("RequestHeaders").asText());
                    requestBlock.set("Headers", requestHeaders);
                }

                if (source.has("RequestPayload")) {
                    requestBlock.put("payload", source.get("RequestPayload").asText());
                }

                transformedData.put("request", requestBlock);
                ObjectNode responseBlock = this.objectMapper.createObjectNode();
                if (source.has("ResponseHeaders")) {
                    JsonNode responseHeaders = this.objectMapper.readTree(source.get("ResponseHeaders").asText());
                    responseBlock.set("Headers", responseHeaders);
                }

                if (source.has("ResponsePayload")) {
                    responseBlock.put("payload", source.get("ResponsePayload").asText());
                }

                transformedData.put("response", responseBlock);
                List<Map<String, String>> customFields = new ArrayList();

                for(ApiMetadataField field : fields) {
                    String identifier = field.getIdentifier();
                    String fieldName = field.getField();
                    String keyStatus = field.getKey_status();
                    Optional<JsonNode> valueNode = this.getValueFromPath(source, identifier);
                    if (valueNode.isPresent()) {
                        String value = ((JsonNode)valueNode.get()).asText();
                        if ("Custom".equalsIgnoreCase(keyStatus)) {
                            Map<String, String> customFieldEntry = new HashMap();
                            customFieldEntry.put("key", fieldName);
                            customFieldEntry.put("value", value);
                            customFields.add(customFieldEntry);
                        } else {
                            transformedData.put(fieldName, value);
                        }
                    }
                }

                if (!customFields.isEmpty()) {
                    transformedData.put("CustomField", customFields);
                }

                if (transformedData.containsKey("RequestTime") && transformedData.containsKey("ResponseTime")) {
                    try {
                        if ("date".equalsIgnoreCase(contentType)) {
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(requestPattern);
                            LocalDateTime requestDateTime = LocalDateTime.parse(transformedData.get("RequestTime").toString(), formatter);
                            LocalDateTime responseDateTime = LocalDateTime.parse(transformedData.get("ResponseTime").toString(), formatter);
                            DateTimeFormatter isoFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
                            transformedData.put("RequestTime", requestDateTime.format(isoFormatter));
                            transformedData.put("ResponseTime", responseDateTime.format(isoFormatter));
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
        } else {
            return null;
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
