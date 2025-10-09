package com.parsing.elasticsearch_parsing.Repository;

import java.util.List;
import java.util.Optional;

import com.parsing.elasticsearch_parsing.Entity.Connection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ConnectionRepository extends JpaRepository<Connection, String> {
    List<Connection> findByConnectionType(String connectiontype);

    Optional<Connection> findByConnectionName(String connectionName);
}
