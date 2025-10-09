package com.parsing.elasticsearch_parsing.Entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(
        name = "connections"
)
public class Connection {
    @Id
    @Column(
            name = "connectionname"
    )
    private String connectionName;
    @Column(
            name = "connectiontype"
    )
    private String connectionType;
    @Column(
            name = "details",
            length = 2000
    )
    private String details;

    public String getConnectiontype() {
        return this.connectionType;
    }

    public void setConnectiontype(String connectiontype) {
        this.connectionType = connectiontype;
    }

    public String getDetails() {
        return this.details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public String getConnectionName() {
        return this.connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }
}