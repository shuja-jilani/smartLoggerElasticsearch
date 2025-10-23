package com.parsing.elasticsearch_parsing.Service.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.parsing.elasticsearch_parsing.Entity.ApiMetadataField;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class FieldExtractorUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final XmlMapper xmlMapper = new XmlMapper();

    private FieldExtractorUtil() {}

    public static String extractValue(JsonNode source, ApiMetadataField field) {
        try {
            // Step 1: Base extraction
            Optional<JsonNode> valueNode = PathUtils.getValueFromPath(source, field.getPath());
            if (valueNode.isEmpty()) return null;

            String rawValue = valueNode.get().asText();
            String sourceContent = field.getSource_content();
            String extractionPath = field.getExtraction_path();

            String value = rawValue;

            // Step 2: Nested extraction (source_content + extraction_path)
            if (("JSON".equalsIgnoreCase(sourceContent) || "XML".equalsIgnoreCase(sourceContent))
                    && extractionPath != null && !extractionPath.isEmpty()) {

                JsonNode innerJson;
                if ("XML".equalsIgnoreCase(sourceContent)) {
                    innerJson = xmlMapper.readTree(rawValue);
                } else {
                    innerJson = objectMapper.readTree(rawValue);
                }

                Optional<JsonNode> extracted = PathUtils.getValueFromPath(innerJson, extractionPath);
                if (extracted.isPresent()) {
                    value = extracted.get().asText();
                }
            }

            // Step 3: Date handling
            String dateType = field.getDate_type();
            String datePattern = field.getDate_pattern();

            if (value != null && dateType != null) {
                try {
                    if ("epoch".equalsIgnoreCase(dateType)) {
                        long epoch = Long.parseLong(value);
                        Instant instant = Instant.ofEpochMilli(epoch);
                        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
                        value = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
                    } else if ("date".equalsIgnoreCase(dateType) && datePattern != null) {
                        DateTimeFormatter inputFmt = DateTimeFormatter.ofPattern(datePattern);
                        LocalDateTime date = LocalDateTime.parse(value, inputFmt);
                        value = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
                    }
                } catch (Exception e) {
                    System.err.println("Date parsing failed for " + field.getField() + ": " + e.getMessage());
                }
            }

            return value;
        } catch (Exception e) {
            System.err.println("Failed to extract value for field " + field.getField() + ": " + e.getMessage());
            return null;
        }
    }
}

