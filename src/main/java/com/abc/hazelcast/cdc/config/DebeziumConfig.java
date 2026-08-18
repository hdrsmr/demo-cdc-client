package com.abc.hazelcast.cdc.config;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;

import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "debezium.enabled", havingValue = "true")
public class DebeziumConfig {

    @Value("${debezium.connector.class}")
    private String connectorClass;

    @Value("${debezium.offset.storage.file.filename}")
    private String offsetStorageFile;

    @Value("${debezium.database.hostname}")
    private String databaseHostname;

    @Value("${debezium.database.port}")
    private String databasePort;

    @Value("${debezium.database.user}")
    private String databaseUser;

    @Value("${debezium.database.password}")
    private String databasePassword;

    @Value("${debezium.database.dbname}")
    private String databaseDbname;

    @Value("${debezium.database.server.name}")
    private String databaseServerName;

    @Value("${debezium.table.include.list}")
    private String tableIncludeList;

    @Value("${debezium.schema.include.list}")
    private String schemaIncludeList;

    @Value("${debezium.offset.flush.interval.ms}")
    private String offsetFlushIntervalMs;

    @Value("${debezium.database.names:TestDb}")
    private String databaseNames;

    @Value("${debezium.topic.prefix:customer-server}")
    private String topicPrefix;

    @Autowired
    private HazelcastInstance hazelcastInstance;

//    private static final String CACHE_NAME = "customers";

    @Bean
    public DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine() {
        // Ensure offset storage directory exists
        try {
            Path offsetPath = Paths.get(offsetStorageFile).getParent();
            if (offsetPath != null && !Files.exists(offsetPath)) {
                Files.createDirectories(offsetPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create offset storage directory", e);
        }

        Properties props = new Properties();

        // Basic connector configuration
        props.setProperty("name", "sqlserver-cdc-connector");
        props.setProperty("connector.class", connectorClass);

        // File-based schema history
        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", "./target/schema_history.dat");

        // Database names and topic prefix
        props.setProperty("database.names", databaseNames);
        props.setProperty("topic.prefix", topicPrefix);

        // Offset storage
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", offsetStorageFile);
        props.setProperty("offset.flush.interval.ms", offsetFlushIntervalMs);

        // Database connection
        props.setProperty("database.hostname", databaseHostname);
        props.setProperty("database.port", databasePort);
        props.setProperty("database.user", databaseUser);
        props.setProperty("database.password", databasePassword);
        props.setProperty("database.dbname", databaseDbname);
        props.setProperty("database.server.name", databaseServerName);
        props.setProperty("database.encrypt", "false");
        props.setProperty("database.trustServerCertificate", "true");

        // Table filtering
        props.setProperty("table.include.list", tableIncludeList);
        props.setProperty("schema.include.list", schemaIncludeList);

        // Snapshot mode
        props.setProperty("snapshot.mode", "schema_only");

        // Other configurations
        props.setProperty("decimal.handling.mode", "double");
        props.setProperty("binary.handling.mode", "bytes");
        props.setProperty("time.precision.mode", "adaptive_time_microseconds");
        props.setProperty("database.history.skip.unparseable.ddl", "true");

        // Di bagian Properties, tambahkan:
        props.setProperty("snapshot.mode", "initial");
        props.setProperty("database.history.skip.unparseable.ddl", "true");
        props.setProperty("schema.history.internal.skip.unparseable.ddl", "true");

        log.info("========================================");
        log.info("Debezium Configuration:");
        log.info("  connector.class: {}", connectorClass);
        log.info("  database.hostname: {}", databaseHostname);
        log.info("  database.port: {}", databasePort);
        log.info("  database.dbname: {}", databaseDbname);
        log.info("  database.names: {}", databaseNames);
        log.info("  topic.prefix: {}", topicPrefix);
        log.info("  table.include.list: {}", tableIncludeList);
        log.info("========================================");

        DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = DebeziumEngine
                .create(ChangeEventFormat.of(Connect.class))
                .using(props)
                .notifying(records -> {
                    SourceRecord sourceRecord = records.record();
                    if (sourceRecord != null) {
                        try {
                            processChangeEvent(sourceRecord);
                        } catch (Exception e) {
                            log.error("Error processing change event: {}", e.getMessage(), e);
                        }
                    }
                })
                .build();

        return engine;
    }

    /**
     * Process change event from Debezium - FINAL VERSION
     */
    private void processChangeEvent(SourceRecord sourceRecord) {
        try {
            Object value = sourceRecord.value();
            if (value == null) {
                log.warn("Received null value in record");
                return;
            }

            if (value instanceof Struct) {
                Struct struct = (Struct) value;

                if (struct.schema().field("op") != null) {
                    processDataChangeEvent(struct);
                } else {
                    log.debug("Schema change event received, skipping...");
                }
            } else {
                log.warn("Value is not a Struct: {}", value.getClass().getName());
            }

        } catch (Exception e) {
            log.error("Error processing change event: {}", e.getMessage(), e);
        }
    }

    /**
     * Process data change event (INSERT, UPDATE, DELETE) - ONLY FOR CRUD
     */
    private void processDataChangeEvent(Struct struct) {
        try {
            String operation = struct.getString("op");

            // SKIP snapshot events (tidak ditampilkan)
            if ("r".equals(operation)) {
                // Snapshot event - tidak ditampilkan
                return;
            }

            //  SKIP jika bukan CRUD (c, u, d)
            if (!"c".equals(operation) && !"u".equals(operation) && !"d".equals(operation)) {
                log.debug("Skipping non-CRUD operation: {}", operation);
                return;
            }

            //  HANYA tampilkan CRUD events
            log.info("=== CRUD OPERATION DETECTED ===");
            log.info("Operation: {}", operation);

            Struct after = struct.getStruct("after");
            Struct before = struct.getStruct("before");

            Struct source = struct.getStruct("source");
            String table = source != null ? source.getString("table") : "unknown";
            //  String db = source != null ? source.getString("db") : "unknown";

            switch (operation) {
                case "c": // CREATE
                    log.info("CREATE - Table: {}", table);
                    if (after != null) {
                        printCustomerData(after, "New");
                        handleCreate(after);
                    }
                    break;

                case "u": // UPDATE
                    log.info("UPDATE - Table: {}", table);
                    if (before != null) {
                        printCustomerData(before, "Before");
                    }
                    if (after != null) {
                        printCustomerData(after, "After");
                        handleUpdate(after);
                    }
                    break;

                case "d": // DELETE
                    log.info("DELETE - Table: {}", table);
                    if (before != null) {
                        printCustomerData(before, "Deleted");
                        handleDelete(before);
                    }
                    break;

                default:
                    log.warn("Unknown operation: {}", operation);
                    break;
            }

        } catch (Exception e) {
            log.error("Error processing data change event: {}", e.getMessage(), e);
        }
    }


    /**
     * Print customer data from Struct
     */
    private void printCustomerData(Struct struct, String label) {
        try {
            if (struct == null) {
                log.warn("  {} Data: null", label);
                return;
            }

            Integer customerId = struct.getInt32("customer_id");
            String customerCode = struct.getString("customer_code");
            String customerName = struct.getString("customer_name");
            String email = struct.getString("email");
            String city = struct.getString("city");

            log.info("  {} Data:", label);
            log.info("    ID: {}", customerId);
            log.info("    Code: {}", customerCode);
            log.info("    Name: {}", customerName);
            log.info("    Email: {}", email);
            log.info("    City: {}", city);

        } catch (Exception e) {
            log.warn("Error printing customer data: {}", e.getMessage());
        }
    }


    private void handleCreate(Struct after) {
        try {
            if (after == null) return;

            Integer customerId = after.getInt32("customer_id");
            String customerCode = after.getString("customer_code");
            String customerName = after.getString("customer_name");
            String email = after.getString("email");
            String city = after.getString("city");

            log.info("📝 New customer created: ID={}, Code={}, Name={}",
                    customerId, customerCode, customerName);

            // ✅ Simpan sebagai JSON dengan String date
            Map<String, Object> customerMap = new HashMap<>();
            customerMap.put("customerId", customerId);
            customerMap.put("customerCode", customerCode);
            customerMap.put("customerName", customerName);
            customerMap.put("email", email);
            customerMap.put("city", city);
            customerMap.put("createdDate", LocalDateTime.now().toString()); // ✅ String
            customerMap.put("updatedDate", LocalDateTime.now().toString()); // ✅ String
            customerMap.put("source", "HAZELCAST_CACHE");

            ObjectMapper mapper = new ObjectMapper();
            String jsonValue = mapper.writeValueAsString(customerMap);
            HazelcastJsonValue jsonCustomer = new HazelcastJsonValue(jsonValue);

            IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
            cache.put(customerId, jsonCustomer);

            log.info("✅ Customer added to Hazelcast cache as JSON: ID={}, Name={}",
                    customerId, customerName);

        } catch (Exception e) {
            log.error("Error handling CREATE event: {}", e.getMessage(), e);
        }
    }

    private void handleUpdate(Struct after) {
        try {
            if (after == null) return;

            Integer customerId = after.getInt32("customer_id");
            String customerCode = after.getString("customer_code");
            String customerName = after.getString("customer_name");
            String email = after.getString("email");
            String city = after.getString("city");

            log.info(" Customer updated: ID={}, Code={}, Name={}",
                    customerId, customerCode, customerName);

            // ✅ Simpan sebagai JSON
            Map<String, Object> customerMap = new HashMap<>();
            customerMap.put("customerId", customerId);
            customerMap.put("customerCode", customerCode);
            customerMap.put("customerName", customerName);
            customerMap.put("email", email);
            customerMap.put("city", city);
            customerMap.put("createdDate", LocalDateTime.now().toString());
            customerMap.put("updatedDate", LocalDateTime.now().toString());
            customerMap.put("source", "HAZELCAST_CACHE_UPDATED");

            ObjectMapper mapper = new ObjectMapper();
            String jsonValue = mapper.writeValueAsString(customerMap);
            HazelcastJsonValue jsonCustomer = new HazelcastJsonValue(jsonValue);

            IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
            cache.put(customerId, jsonCustomer);

            log.info("Customer updated in Hazelcast cache: ID={}, Name={}",
                    customerId, customerName);

        } catch (Exception e) {
            log.error("Error handling UPDATE event: {}", e.getMessage(), e);
        }
    }

    private void handleDelete(Struct before) {
        try {
            if (before == null) return;

            Integer customerId = before.getInt32("customer_id");
            String customerCode = before.getString("customer_code");

            log.info("📝 Customer deleted: ID={}, Code={}", customerId, customerCode);

            // ✅ Hapus dari cache
            IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
            cache.remove(customerId);

            log.info("✅ Customer removed from Hazelcast cache: ID={}", customerId);

        } catch (Exception e) {
            log.error("Error handling DELETE event: {}", e.getMessage(), e);
        }
    }


}
