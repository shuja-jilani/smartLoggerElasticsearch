package com.parsing.elasticsearch_parsing.Repository;

import java.util.List;
import java.util.UUID;

import com.parsing.elasticsearch_parsing.Entity.ApiMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ApiMetadataRepository extends JpaRepository<ApiMetadata, UUID> {
    List<ApiMetadata> findByConnectionNameAndStatus(String connectionName, String status);

    ApiMetadata findByResourcePath(String resourcePath);
}

