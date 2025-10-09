package com.parsing.elasticsearch_parsing.Service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.parsing.elasticsearch_parsing.Entity.Connection;
import com.parsing.elasticsearch_parsing.Repository.ApiMetadataFieldRepository;
import com.parsing.elasticsearch_parsing.Repository.ApiMetadataRepository;
import com.parsing.elasticsearch_parsing.Repository.ConnectionRepository;
import lombok.Data;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

@Data
@Service
public class DatabaseService {
    private final ConnectionRepository connectionRepository;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ApiMetadataRepository apiMetadataRepository;
    private final ApiMetadataFieldRepository apiMetadataFieldRepository;

    public boolean checkIfApiExists(String apiName) {
        String sql = "SELECT COUNT(*) FROM api_metadata WHERE api_name = ?";
        Integer count = (Integer)this.jdbcTemplate.queryForObject(sql, Integer.class, new Object[]{apiName});
        return count != null && count > 0;
    }

    public void insertApiMetadata(UUID uniqueId, String connectionName, String dataset, String apiName, String roleNames, String status, String resourcePath) {
        String sql = "INSERT INTO api_metadata (unique_id, connection_name, dataset, api_name, role_names, status, resource_path) VALUES (?, ?, ?, ?, ?, ?, ?)";
        this.jdbcTemplate.update(sql, new Object[]{uniqueId, connectionName, dataset, apiName, roleNames, status, resourcePath});
    }

    public void insertApiMetadataField(UUID apiMetadataId, String fieldName, String identifier, String datatype, String contentType, String keyStatus, String path) {
        String sql = "INSERT INTO api_metadata_field (api_metadata_id, field, identifier, datatype, content_type, key_status, path) VALUES (?, ?, ?, ?, ?, ?, ?)";
        this.jdbcTemplate.update(sql, new Object[]{apiMetadataId, fieldName, identifier, datatype, contentType, keyStatus, path});
    }

    public List<Map<String, Object>> fetchTableData(Connection connection, String gte, String lte) throws Exception {
        JsonNode details = this.objectMapper.readTree(connection.getDetails());
        String host = details.get("host").asText();
        int port = details.get("port").asInt();
        String dbName = details.get("databaseName").asText();
        String user = details.get("userName").asText();
        String password = details.get("password").asText();
        String tableName = details.get("tableName").asText();
        String requestTimeColumn = "RequestTime";

        for(JsonNode field : details.get("fields")) {
            if ("RequestTime".equals(field.get("field").asText())) {
                requestTimeColumn = field.get("identifier").asText();
                break;
            }
        }

        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, dbName);
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        dataSource.setDriverClassName("org.postgresql.Driver");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = String.format("SELECT * FROM \"%s\" WHERE \"%s\" >= ? AND \"%s\" <= ?", tableName, requestTimeColumn, requestTimeColumn);
        Timestamp gteTimestamp = Timestamp.valueOf(gte.replace("T", " "));
        Timestamp lteTimestamp = Timestamp.valueOf(lte.replace("T", " "));
        return jdbcTemplate.queryForList(sql, new Object[]{gteTimestamp, lteTimestamp});
    }
}