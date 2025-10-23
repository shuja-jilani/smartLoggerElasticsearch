package com.parsing.elasticsearch_parsing.Service.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

public class PathUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private PathUtils() {}

    public static Optional<JsonNode> getValueFromPath(JsonNode root, String path) {
        if (root == null || path == null || path.isEmpty()) return Optional.empty();

        String[] parts = path.split("\\.");
        JsonNode current = root;
        for (String part : parts) {
            if (current == null) return Optional.empty();
            current = current.path(part);
        }
        return current.isMissingNode() ? Optional.empty() : Optional.of(current);
    }
}

