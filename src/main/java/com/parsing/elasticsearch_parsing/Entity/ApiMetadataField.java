package com.parsing.elasticsearch_parsing.Entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.util.UUID;

@Entity
@Table(
        name = "api_metadata_field"
)
public class ApiMetadataField {
    @Id
    @GeneratedValue(
            strategy = GenerationType.IDENTITY
    )
    private Long id;
    @Column(
            name = "api_metadata_id"
    )
    private UUID apiMetadataId;
    private String field;
    private String identifier;
    private String path;
    private String datatype;
    private String contentType;
    private String key_status;
    private String source_content;
    private String extraction_path;
    private String date_type;
    private String date_pattern;
    @Transient
    private boolean wmAPIGateway;

    public String getDate_type() {
        return date_type;
    }

    public void setDate_type(String date_type) {
        this.date_type = date_type;
    }

    public String getExtraction_path() {
        return extraction_path;
    }

    public void setExtraction_path(String extraction_path) {
        this.extraction_path = extraction_path;
    }

    public String getSource_content() {
        return source_content;
    }

    public void setSource_content(String source_content) {
        this.source_content = source_content;
    }

    public String getDate_pattern() {
        return date_pattern;
    }

    public void setDate_pattern(String date_pattern) {
        this.date_pattern = date_pattern;
    }
    public String getKey_status() {
        return this.key_status;
    }

    public void setKey_status(String key_status) {
        this.key_status = key_status;
    }

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public UUID getApiMetadataId() {
        return this.apiMetadataId;
    }

    public void setApiMetadataId(UUID apiMetadataId) {
        this.apiMetadataId = apiMetadataId;
    }

    public String getField() {
        return this.field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getIdentifier() {
        return this.identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getPath() {
        return this.path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getDatatype() {
        return this.datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public String getContentType() {
        return this.contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public boolean isWMAPIGateway() {
        return this.wmAPIGateway;
    }

    public void setWMAPIGateway(boolean wmAPIGateway) {
        this.wmAPIGateway = wmAPIGateway;
    }
}
