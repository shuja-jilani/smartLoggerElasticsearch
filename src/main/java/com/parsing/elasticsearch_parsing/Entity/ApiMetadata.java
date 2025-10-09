package com.parsing.elasticsearch_parsing.Entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;

@Entity
@Table(
        name = "api_metadata"
)
public class ApiMetadata {
    @Id
    @GeneratedValue(
            strategy = GenerationType.AUTO
    )
    private UUID uniqueId;
    private String api_name;
    @Column(
            name = "connection_name"
    )
    private String connectionName;
    @Column(
            name = "dataset"
    )
    private String dataset;
    @Column(
            name = "role_names"
    )
    private String roleNames;
    @Column(
            name = "resource_path"
    )
    private String resourcePath;
    @Column(
            name = "status"
    )
    private String status;

    public String getApi_name() {
        return this.api_name;
    }

    public void setApi_name(String api_name) {
        this.api_name = api_name;
    }

    public UUID getUniqueId() {
        return this.uniqueId;
    }

    public void setUniqueId(UUID uniqueId) {
        this.uniqueId = uniqueId;
    }

    public String getConnectionName() {
        return this.connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getDataset() {
        return this.dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getStatus() {
        return this.status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getResourcePath() {
        return this.resourcePath;
    }

    public void setResourcePath(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    public String getRoleNames() {
        return this.roleNames;
    }

    public void setRoleNames(String roleNames) {
        this.roleNames = roleNames;
    }
}
