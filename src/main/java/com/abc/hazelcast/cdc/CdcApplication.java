package com.abc.hazelcast.cdc;


import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
@OpenAPIDefinition(
        info = @Info(
                title = "CDC with Debezium and Hazelcast API",
                version = "1.0.0",
                description = "Change Data Capture with Debezium Embedded, Hazelcast Cache, and Spring Boot"
        ),
        servers = @Server(url = "http://localhost:8888", description = "Local Server")
)
public class CdcApplication {

    public static void main(String[] args) {
        SpringApplication.run(CdcApplication.class, args);
    }
}