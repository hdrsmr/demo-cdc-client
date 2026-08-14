package com.abc.hazelcast.cdc.listener;


import com.abc.hazelcast.cdc.dto.CustomerResponse;
import com.abc.hazelcast.cdc.service.CustomerService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "debezium.enabled", havingValue = "true")
public class CdcEventListener {

    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;
    private final CustomerService customerService;
    private final ObjectMapper objectMapper;

    @Value("${debezium.enabled:true}")
    private boolean debeziumEnabled;

    private ExecutorService executor;

    @PostConstruct
    public void start() {
        if (!debeziumEnabled) {
            log.info("Debezium is disabled");
            return;
        }

        log.info("Starting Debezium CDC listener...");
        executor = Executors.newSingleThreadExecutor();

        executor.submit(() -> {
            try {
                debeziumEngine.run();
            } catch (Exception e) {
                log.error("Error running Debezium engine", e);
            }
        });

        log.info("Debezium CDC listener started successfully");
    }

    @PreDestroy
    public void stop() {
        if (!debeziumEnabled || executor == null) {
            return;
        }

        log.info("Stopping Debezium CDC listener...");
        try {
            try {
                debeziumEngine.close();
            } catch (Exception e) {
                log.error("Error closing Debezium engine", e);
            }
            
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
            log.info("Debezium CDC listener stopped");
        } catch (Exception e) {
            log.error("Error stopping Debezium engine", e);
        }
    }

    public void processChangeEvents(List<SourceRecord> records) {
        log.info("Processing {} change events from Debezium", records.size());

        for (SourceRecord record : records) {
            try {
                processChangeEvent(record);
            } catch (Exception e) {
                log.error("Error processing change event", e);
            }
        }
    }

    private void processChangeEvent(SourceRecord record) {
        try {
            Object value = record.value();
            if (value == null) {
                log.warn("Received null value in record");
                return;
            }

            String json = objectMapper.writeValueAsString(value);
            JsonNode root = objectMapper.readTree(json);
            
            JsonNode payload = root.get("payload");
            if (payload == null) {
                log.warn("No payload in record");
                return;
            }

            String operation = payload.has("op") ? payload.get("op").asText() : "unknown";
            JsonNode after = payload.get("after");
            
            if (after == null) {
                log.debug("No 'after' data for operation: {}", operation);
                return;
            }

            String customerCode = getCustomerCode(after);
            Integer customerId = getCustomerId(after);

            switch (operation) {
                case "c":
                    log.info("✅ CDC CREATE - Customer: {} ({})", customerCode, customerId);
                    handleCreate(after);
                    break;
                case "u":
                    log.info("🔄 CDC UPDATE - Customer: {} ({})", customerCode, customerId);
                    handleUpdate(after);
                    break;
                case "d":
                    log.info("❌ CDC DELETE - Customer: {} ({})", customerCode, customerId);
                    handleDelete(customerId);
                    break;
                case "r":
                    log.info("📖 CDC READ/SNAPSHOT - Customer: {} ({})", customerCode, customerId);
                    handleCreate(after);
                    break;
                default:
                    log.warn("Unknown operation: {}", operation);
                    break;
            }
        } catch (Exception e) {
            log.error("Error processing change event", e);
        }
    }

    private void handleCreate(JsonNode after) {
        try {
            CustomerResponse customer = parseCustomer(after);
            if (customer != null) {
                customerService.evictCache(customer.getCustomerCode());
                customerService.evictCache(String.valueOf(customer.getCustomerId()));
                log.info("Cache updated for customer: {} ({})", 
                    customer.getCustomerName(), customer.getCustomerCode());
            }
        } catch (Exception e) {
            log.error("Error handling create event", e);
        }
    }

    private void handleUpdate(JsonNode after) {
        try {
            CustomerResponse customer = parseCustomer(after);
            if (customer != null) {
                customerService.evictCache(customer.getCustomerCode());
                customerService.evictCache(String.valueOf(customer.getCustomerId()));
                log.info("Cache updated for customer: {} ({})", 
                    customer.getCustomerName(), customer.getCustomerCode());
            }
        } catch (Exception e) {
            log.error("Error handling update event", e);
        }
    }

    private void handleDelete(Integer customerId) {
        if (customerId != null) {
            customerService.evictCache(String.valueOf(customerId));
            log.info("Cache evicted for customer ID: {}", customerId);
        }
    }

    private String getCustomerCode(JsonNode after) {
        if (after != null && after.has("CustomerCode")) {
            return after.get("CustomerCode").asText();
        }
        return null;
    }

    private Integer getCustomerId(JsonNode after) {
        if (after != null && after.has("CustomerId")) {
            return after.get("CustomerId").asInt();
        }
        return null;
    }

    private CustomerResponse parseCustomer(JsonNode after) {
        try {
            if (after == null) {
                return null;
            }

            CustomerResponse response = new CustomerResponse();
            
            if (after.has("CustomerId")) {
                response.setCustomerId(after.get("CustomerId").asInt());
            }
            if (after.has("CustomerCode")) {
                response.setCustomerCode(after.get("CustomerCode").asText());
            }
            if (after.has("CustomerName")) {
                response.setCustomerName(after.get("CustomerName").asText());
            }
            if (after.has("Email")) {
                response.setEmail(after.get("Email").asText());
            }
            if (after.has("City")) {
                response.setCity(after.get("City").asText());
            }
            response.setSource("HAZELCAST_CACHE");

            return response;
        } catch (Exception e) {
            log.error("Error parsing customer from JSON", e);
            return null;
        }
    }
    
    //add
 // Di CdcEventListener.java
    public void processSingleEvent(SourceRecord record) {
        // Proses single record langsung
        // Bisa menggunakan method yang sama dengan processChangeEvents
        processChangeEvents(List.of(record));
   }
}
//import com.abc.hazelcast.cdc.dto.CustomerResponse;
//import com.abc.hazelcast.cdc.service.CustomerService;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.debezium.engine.DebeziumEngine;
//import io.debezium.engine.RecordChangeEvent;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.connect.source.SourceRecord;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
//import org.springframework.stereotype.Component;
//
//import jakarta.annotation.PostConstruct;
//import jakarta.annotation.PreDestroy;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//
//@Slf4j
//@Component
//@RequiredArgsConstructor
//@ConditionalOnProperty(name = "debezium.enabled", havingValue = "true")
//public class CdcEventListener {
//
//    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;
//    private final CustomerService customerService;
//    private final ObjectMapper objectMapper;
//
//    @Value("${debezium.enabled:true}")
//    private boolean debeziumEnabled;
//
//    private ExecutorService executor;
//
//    @PostConstruct
//    public void start() {
//        if (!debeziumEnabled) {
//            log.info("Debezium is disabled");
//            return;
//        }
//
//        log.info("Starting Debezium CDC listener...");
//        executor = Executors.newSingleThreadExecutor();
//
//        executor.submit(() -> {
//            try {
//                // Start the engine
//                debeziumEngine.run();
//            } catch (Exception e) {
//                log.error("Error running Debezium engine", e);
//            }
//        });
//
//        log.info("Debezium CDC listener started successfully");
//    }
//
//    @PreDestroy
//    public void stop() {
//        if (!debeziumEnabled || executor == null) {
//            return;
//        }
//
//        log.info("Stopping Debezium CDC listener...");
//        try {
//            // Close the engine
//            try {
//                debeziumEngine.close();
//            } catch (Exception e) {
//                log.error("Error closing Debezium engine", e);
//            }
//            
//            executor.shutdown();
//            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
//                executor.shutdownNow();
//            }
//            log.info("Debezium CDC listener stopped");
//        } catch (Exception e) {
//            log.error("Error stopping Debezium engine", e);
//        }
//    }
//
//    // This method will be called by Debezium engine via the change consumer
//    public void processChangeEvents(List<SourceRecord> records) {
//        log.info("Processing {} change events from Debezium", records.size());
//
//        for (SourceRecord record : records) {
//            try {
//                processChangeEvent(record);
//            } catch (Exception e) {
//                log.error("Error processing change event", e);
//            }
//        }
//    }
//
//    private void processChangeEvent(SourceRecord record) {
//        String topic = record.topic();
//        log.debug("Processing event from topic: {}", topic);
//
//        // Extract operation type
//        Object value = record.value();
//        if (value == null) {
//            log.warn("Received null value in record");
//            return;
//        }
//
//        // Convert to JSON if possible
//        try {
//            String json = objectMapper.writeValueAsString(value);
//            JsonNode root = objectMapper.readTree(json);
//            
//            JsonNode payload = root.get("payload");
//            if (payload == null) {
//                log.warn("No payload in record");
//                return;
//            }
//
//            String operation = payload.has("op") ? payload.get("op").asText() : "unknown";
//            JsonNode after = payload.get("after");
//            
//            if (after == null) {
//                log.debug("No 'after' data for operation: {}", operation);
//                return;
//            }
//
//            String customerCode = getCustomerCode(after);
//            Integer customerId = getCustomerId(after);
//
//            switch (operation) {
//                case "c": // Create
//                    log.info("✅ CDC CREATE - Customer: {} ({})", customerCode, customerId);
//                    handleCreate(after);
//                    break;
//
//                case "u": // Update
//                    log.info("🔄 CDC UPDATE - Customer: {} ({})", customerCode, customerId);
//                    handleUpdate(after);
//                    break;
//
//                case "d": // Delete
//                    log.info("❌ CDC DELETE - Customer: {} ({})", customerCode, customerId);
//                    handleDelete(customerId);
//                    break;
//
//                case "r": // Read (snapshot)
//                    log.info("📖 CDC READ/SNAPSHOT - Customer: {} ({})", customerCode, customerId);
//                    handleCreate(after);
//                    break;
//
//                default:
//                    log.warn("Unknown operation: {}", operation);
//                    break;
//            }
//        } catch (Exception e) {
//            log.error("Error processing change event", e);
//        }
//    }
//
//    private void handleCreate(JsonNode after) {
//        try {
//            CustomerResponse customer = parseCustomer(after);
//            if (customer != null) {
//                customerService.evictCache(customer.getCustomerCode());
//                customerService.evictCache(String.valueOf(customer.getCustomerId()));
//                log.info("Cache updated for customer: {} ({})", 
//                    customer.getCustomerName(), customer.getCustomerCode());
//            }
//        } catch (Exception e) {
//            log.error("Error handling create event", e);
//        }
//    }
//
//    private void handleUpdate(JsonNode after) {
//        try {
//            CustomerResponse customer = parseCustomer(after);
//            if (customer != null) {
//                customerService.evictCache(customer.getCustomerCode());
//                customerService.evictCache(String.valueOf(customer.getCustomerId()));
//                log.info("Cache updated for customer: {} ({})", 
//                    customer.getCustomerName(), customer.getCustomerCode());
//            }
//        } catch (Exception e) {
//            log.error("Error handling update event", e);
//        }
//    }
//
//    private void handleDelete(Integer customerId) {
//        if (customerId != null) {
//            customerService.evictCache(String.valueOf(customerId));
//            log.info("Cache evicted for customer ID: {}", customerId);
//        }
//    }
//
//    private String getCustomerCode(JsonNode after) {
//        if (after != null && after.has("CustomerCode")) {
//            return after.get("CustomerCode").asText();
//        }
//        return null;
//    }
//
//    private Integer getCustomerId(JsonNode after) {
//        if (after != null && after.has("CustomerId")) {
//            return after.get("CustomerId").asInt();
//        }
//        return null;
//    }
//
//    private CustomerResponse parseCustomer(JsonNode after) {
//        try {
//            if (after == null) {
//                return null;
//            }
//
//            CustomerResponse response = new CustomerResponse();
//            
//            if (after.has("CustomerId")) {
//                response.setCustomerId(after.get("CustomerId").asInt());
//            }
//            if (after.has("CustomerCode")) {
//                response.setCustomerCode(after.get("CustomerCode").asText());
//            }
//            if (after.has("CustomerName")) {
//                response.setCustomerName(after.get("CustomerName").asText());
//            }
//            if (after.has("Email")) {
//                response.setEmail(after.get("Email").asText());
//            }
//            if (after.has("City")) {
//                response.setCity(after.get("City").asText());
//            }
//            response.setSource("HAZELCAST_CACHE");
//
//            return response;
//        } catch (Exception e) {
//            log.error("Error parsing customer from JSON", e);
//            return null;
//        }
//    }
//}