package com.parsing.elasticsearch_parsing.Repository;

import java.util.List;
import java.util.UUID;

import com.parsing.elasticsearch_parsing.Entity.ApiMetadataField;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ApiMetadataFieldRepository extends JpaRepository<ApiMetadataField, Long> {
    List<ApiMetadataField> findByApiMetadataId(UUID apiMetadataId);
}