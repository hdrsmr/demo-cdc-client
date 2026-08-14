package com.abc.hazelcast.cdc.service.impl;

import com.abc.hazelcast.cdc.dto.CustomerResponse;
import com.abc.hazelcast.cdc.service.CacheService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class CacheServiceImpl implements CacheService {

    private final HazelcastInstance hazelcastInstance;
    
    private ObjectMapper objectMapper;

    @PostConstruct
    public void init() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        log.info(" CacheService initialized with ObjectMapper");
    }

    // ============================================
    // TEMP CACHE - STRING
    // ============================================

    @Override
    public void addTempCache(String key, String value) {
        try {
            IMap<String, String> tempCache = hazelcastInstance.getMap("temp-cache");
            tempCache.put(key, value, 5, TimeUnit.MINUTES);
            log.info(" Temp cache added: key={}, value={}, TTL: 5 minutes", key, value);
        } catch (Exception e) {
            log.error(" Failed to add temp cache: {}", e.getMessage());
            throw new RuntimeException("Failed to add temp cache: " + e.getMessage());
        }
    }

    @Override
    public String getTempCache(String key) {
        try {
            IMap<String, String> tempCache = hazelcastInstance.getMap("temp-cache");
            String value = tempCache.get(key);
            log.info(" Temp cache retrieved: key={}, value={}", key, value);
            return value;
        } catch (Exception e) {
            log.error(" Failed to get temp cache: {}", e.getMessage());
            return null;
        }
    }

    // ============================================
    // TEMP CACHE - OBJECT (Map)
    // ============================================
   @Override
    public void addTempCacheMap(String key, Map<String, Object> value) {
        try {
            IMap<String, String> tempCache = hazelcastInstance.getMap("temp-cache");
            
            //  Convert Map to JSON string
            String jsonValue = objectMapper.writeValueAsString(value);
            tempCache.put(key, jsonValue, 5, TimeUnit.MINUTES);
            log.info(" Temp cache object added: key={}, TTL: 5 minutes", key);
        } catch (Exception e) {
            log.error(" Failed to add temp cache object: {}", e.getMessage());
            throw new RuntimeException("Failed to add temp cache object: " + e.getMessage());
        }
    }

   @Override
    public Map<String, Object> getTempCacheMap(String key) {
        try {
            IMap<String, String> tempCache = hazelcastInstance.getMap("temp-cache");
            String jsonValue = tempCache.get(key);
            if (jsonValue == null) {
                log.warn(" Temp cache object not found: key={}", key);
                return null;
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> result = objectMapper.readValue(jsonValue, Map.class);
            log.info(" Temp cache object retrieved: key={}", key);
            return result;
        } catch (Exception e) {
            log.error(" Failed to get temp cache object: {}", e.getMessage());
            return null;
        }
    }

    // ============================================
    // CUSTOMER CACHE
    // ============================================

   @Override
    public void addCustomerWithTTL(Integer customerId, CustomerResponse customer) {
        try {
            IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
            
            Map<String, Object> customerMap = new HashMap<>();
            customerMap.put("customerId", customer.getCustomerId());
            customerMap.put("customerCode", customer.getCustomerCode());
            customerMap.put("customerName", customer.getCustomerName());
            customerMap.put("email", customer.getEmail());
            customerMap.put("city", customer.getCity());
            customerMap.put("createdDate", LocalDateTime.now().toString());
            customerMap.put("source", "CACHE_TTL");
            
            String json = objectMapper.writeValueAsString(customerMap);
            HazelcastJsonValue jsonValue = new HazelcastJsonValue(json);
            
            cache.put(customerId, jsonValue, 30, TimeUnit.MINUTES);
            log.info(" Customer added to cache with TTL 30 minutes: ID={}", customerId);
            
        } catch (Exception e) {
            log.error(" Failed to add customer to cache: {}", e.getMessage());
            throw new RuntimeException("Failed to add customer to cache: " + e.getMessage());
        }
    }

   @Override
    public CustomerResponse getCustomerFromCache(Integer customerId) {
        try {
            IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
            
            HazelcastJsonValue value = cache.get(customerId);
            if (value == null) {
                log.warn(" Customer not found in cache: ID={}", customerId);
                return null;
            }
            
            return objectMapper.readValue(value.toString(), CustomerResponse.class);
            
        } catch (Exception e) {
            log.error(" Failed to get customer from cache: {}", e.getMessage());
            return null;
        }
    }

    // ============================================
    //  CACHE STATISTICS
    // ============================================

   @Override
    public Map<String, Object> getCacheStats() {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            IMap<Integer, HazelcastJsonValue> customerCache = hazelcastInstance.getMap("customers");
            IMap<String, String> tempCache = hazelcastInstance.getMap("temp-cache");
            
            stats.put("customers_size", customerCache.size());
            stats.put("temp_cache_size", tempCache.size());
            stats.put("customers_keys", customerCache.keySet());
            stats.put("temp_cache_keys", tempCache.keySet());
            
            log.info(" Cache stats: customers={}, temp={}", customerCache.size(), tempCache.size());
        } catch (Exception e) {
            log.error(" Failed to get cache stats: {}", e.getMessage());
            stats.put("error", e.getMessage());
        }
        
        return stats;
    }
    

    // ============================================
    // CLEAR ALL CACHE
    // ============================================

    @Override
    public void clearAllCache() {
        try {
            IMap<Integer, HazelcastJsonValue> customerCache = hazelcastInstance.getMap("customers");
            IMap<String, String> tempCache = hazelcastInstance.getMap("temp-cache");
            
            int customerSize = customerCache.size();
            int tempSize = tempCache.size();
            
            customerCache.clear();
            tempCache.clear();
            
            log.info(" All cache cleared: {} customers, {} temp items removed", customerSize, tempSize);
        } catch (Exception e) {
            log.error(" Failed to clear cache: {}", e.getMessage());
            throw new RuntimeException("Failed to clear cache: " + e.getMessage());
        }
    }

    // ============================================
    // EVICT CACHE BY KEY
    // ============================================

    @Override
    public void evictCache(String key) {
        try {
            IMap<Integer, HazelcastJsonValue> customerCache = hazelcastInstance.getMap("customers");
            IMap<String, String> tempCache = hazelcastInstance.getMap("temp-cache");
            
            // Coba hapus dari customer cache (key adalah integer)
            try {
                Integer intKey = Integer.parseInt(key);
                if (customerCache.containsKey(intKey)) {
                    customerCache.remove(intKey);
                    log.info(" Cache evicted from customers: key={}", key);
                    return;
                }
            } catch (NumberFormatException e) {
                // Key bukan integer, lanjut ke temp cache
            }
            
            // Coba hapus dari temp cache (key adalah string)
            if (tempCache.containsKey(key)) {
                tempCache.remove(key);
                log.info(" Cache evicted from temp-cache: key={}", key);
                return;
            }
            
            log.warn(" Cache key not found: {}", key);
            
        } catch (Exception e) {
            log.error(" Failed to evict cache: {}", e.getMessage());
            throw new RuntimeException("Failed to evict cache: " + e.getMessage());
        }
    }

    // ============================================
    //  EVICT CUSTOMER BY ID
    // ============================================

    @Override
    public void evictCustomer(Integer customerId) {
        try {
            IMap<Integer, HazelcastJsonValue> customerCache = hazelcastInstance.getMap("customers");
            
            if (customerCache.containsKey(customerId)) {
                customerCache.remove(customerId);
                log.info(" Customer cache evicted: ID={}", customerId);
            } else {
                log.warn(" Customer not found in cache: ID={}", customerId);
            }
            
        } catch (Exception e) {
            log.error(" Failed to evict customer: {}", e.getMessage());
            throw new RuntimeException("Failed to evict customer: " + e.getMessage());
        }
    }

  
}