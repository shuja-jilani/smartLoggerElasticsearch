package com.parsing.elasticsearch_parsing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ElasticsearchParsingApplication {

    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchParsingApplication.class, args);
    }

}
