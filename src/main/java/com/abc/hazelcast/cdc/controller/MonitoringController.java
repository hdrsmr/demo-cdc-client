package com.abc.hazelcast.cdc.controller;


import com.abc.hazelcast.cdc.service.ConnectionPoolMonitor;
import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/monitoring")
@RequiredArgsConstructor
public class MonitoringController {

    private final ConnectionPoolMonitor connectionPoolMonitor;

    @Operation(summary = "Get connection pool statistics")
    @GetMapping("/pool-stats")
    public ResponseEntity<Map<String, Object>> getPoolStats() {
        log.info("GET /api/monitoring/pool-stats - Getting connection pool statistics");
        return ResponseEntity.ok(connectionPoolMonitor.getPoolStats());
    }
}