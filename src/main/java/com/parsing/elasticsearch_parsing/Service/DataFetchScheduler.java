package com.parsing.elasticsearch_parsing.Service;


import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class DataFetchScheduler {

    private final ConnectionPublisherService connectionPublisherService;

    public DataFetchScheduler(ConnectionPublisherService connectionPublisherService) {
        this.connectionPublisherService = connectionPublisherService;
    }


    @Scheduled(fixedRate = 120000) // Runs every 2 minutes
    public void publishConnectionsAutomatically() {
         try {
             String message = connectionPublisherService.publishConnectionsToKafka();
             System.out.println("Auto Publish (Connections): " + message);
         } catch (Exception e) {
             System.err.println("Error during scheduled publish: " + e.getMessage());
             e.printStackTrace();
         }

     }
}

