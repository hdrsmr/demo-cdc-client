package com.abc.hazelcast.cdc.controller;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.abc.hazelcast.cdc.dto.CustomerResponse;
import com.abc.hazelcast.cdc.service.CacheService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/cache")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Cache Management", description = "APIs for cache operations with LRU & TTL")
public class CacheController {

    private final CacheService cacheService;
    
 //  Inject HazelcastInstance dan ObjectMapper
    private final HazelcastInstance hazelcastInstance;
    private final ObjectMapper objectMapper;


    // POST untuk menambah temp cache (String)
    @Operation(summary = "Add temp cache with 5 minutes TTL")
    @PostMapping("/temp/{key}")
    public ResponseEntity<String> addTempCache(
            @PathVariable String key,
            @RequestBody String value) {
        
        cacheService.addTempCache(key, value);
        return ResponseEntity.ok("Temp cache added with TTL 5 minutes");
    }

    // GET untuk mengambil temp cache
    @Operation(summary = "Get temp cache")
    @GetMapping("/temp/{key}")
    public ResponseEntity<String> getTempCache(@PathVariable String key) {
        String value = cacheService.getTempCache(key);
        if (value == null) {
            log.warn(" Temp cache not found: key={}", key);
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(value);
    }

    //  POST untuk menambah temp cache (Object/Map)
    @Operation(summary = "Add temp cache object with 5 minutes TTL")
    @PostMapping("/temp-object/{key}")
    public ResponseEntity<String> addTempCacheObject(
            @PathVariable String key,
            @RequestBody Map<String, Object> value) {
        
        cacheService.addTempCacheMap(key, value);
        return ResponseEntity.ok("Temp cache object added with TTL 5 minutes");
    }

    //  GET untuk mengambil temp cache object
    @Operation(summary = "Get temp cache object")
    @GetMapping("/temp-object/{key}")
    public ResponseEntity<Map<String, Object>> getTempCacheObject(@PathVariable String key) {
        Map<String, Object> value = cacheService.getTempCacheMap(key);
        if (value == null) {
            log.warn(" Temp cache object not found: key={}", key);
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(value);
    }

    //  POST untuk menambah customer dengan TTL
    @Operation(summary = "Add customer with TTL")
    @PostMapping("/customer/{id}")
    public ResponseEntity<String> addCustomerWithTTL(
            @PathVariable Integer id,
            @RequestBody CustomerResponse customer) {
        
        cacheService.addCustomerWithTTL(id, customer);
        return ResponseEntity.ok("Customer added with TTL 30 minutes");
    }

    //  GET untuk mengambil customer dari cache
    @Operation(summary = "Get customer from cache")
    @GetMapping("/customer/{id}")
    public ResponseEntity<CustomerResponse> getCustomerFromCache(@PathVariable Integer id) {
        CustomerResponse customer = cacheService.getCustomerFromCache(id);
        if (customer == null) {
            log.warn(" Customer not found in cache: ID={}", id);
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(customer);
    }

    //  GET cache statistics
    @Operation(summary = "Get cache statistics")
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getCacheStats() {
        return ResponseEntity.ok(cacheService.getCacheStats());
    }
    

    @Operation(summary = "Clear all cache")
    @DeleteMapping("/clear")
    public ResponseEntity<String> clearAllCache() {
        log.info("DELETE /api/cache/clear - Clearing all cache");
        cacheService.clearAllCache();
        return ResponseEntity.ok("All cache cleared successfully");
    }

    // ============================================
    // ENDPOINT EVICT BY KEY
    // ============================================

    @Operation(summary = "Evict cache by key")
    @DeleteMapping("/evict/{key}")
    public ResponseEntity<String> evictCache(@PathVariable String key) {
        log.info("DELETE /api/cache/evict/{} - Evicting cache", key);
        cacheService.evictCache(key);
        return ResponseEntity.ok("Cache evicted for key: " + key);
    }

    // ============================================
    // ENDPOINT EVICT CUSTOMER
    // ============================================

    @Operation(summary = "Evict customer by ID")
    @DeleteMapping("/customer/{id}")
    public ResponseEntity<String> evictCustomer(@PathVariable Integer id) {
        log.info("DELETE /api/cache/customer/{} - Evicting customer", id);
        cacheService.evictCustomer(id);
        return ResponseEntity.ok("Customer cache evicted for ID: " + id);
    }

    
    //
    @Operation(summary = "Copy all customers from customers map to temp-cache")
    @PostMapping("/copy-all-from-customer")
    public ResponseEntity<String> copyAllFromCustomerToTemp() {
        log.info("Copying all customers to temp-cache");
        
        try {
            IMap<Integer, HazelcastJsonValue> customerCache = hazelcastInstance.getMap("customers");
            IMap<String, String> tempCache = hazelcastInstance.getMap("temp-cache");
            
            int count = 0;
            for (Map.Entry<Integer, HazelcastJsonValue> entry : customerCache.entrySet()) {
                try {
                    String json = entry.getValue().toString();
                    Map<String, Object> customerMap = objectMapper.readValue(json, Map.class);
                    
                    // Gunakan customerCode sebagai key
                    String key = (String) customerMap.get("customerCode");
                    String valueJson = objectMapper.writeValueAsString(customerMap);
                    
                    tempCache.put(key, valueJson, 5, TimeUnit.MINUTES);
                    count++;
                    
                    log.info(" Copied customer: {} -> {}", key, customerMap.get("customerName"));
                    
                } catch (Exception e) {
                    log.error(" Failed to copy customer: {}", e.getMessage());
                }
            }
            
            log.info(" Copied {} customers to temp-cache", count);
            return ResponseEntity.ok("Copied " + count + " customers to temp-cache");
            
        } catch (Exception e) {
            log.error(" Failed to copy all customers: {}", e.getMessage());
            return ResponseEntity.internalServerError().body("Failed to copy: " + e.getMessage());
        }
    }
}