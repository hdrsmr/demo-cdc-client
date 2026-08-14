//package com.abc.hazelcast.cdc.controller;
//
//import java.time.LocalDateTime;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.http.ResponseEntity;
//import org.springframework.web.bind.annotation.DeleteMapping;
//import org.springframework.web.bind.annotation.GetMapping;
//import org.springframework.web.bind.annotation.PostMapping;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RestController;
//
////import com.abc.hazelcast.cdc.config.DebeziumConfigV2;
////import com.hazelcast.shaded.com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.core.HazelcastJsonValue;
//import com.hazelcast.map.IMap;
//
//import io.swagger.v3.oas.annotations.tags.Tag;
//import lombok.extern.slf4j.Slf4j;
//
//
//    @Slf4j
//    @Tag(name = "Customer Management Test", description = "APIs add-cache")
//	@RestController
//	@RequestMapping("/api/test")
//	public class InsertManualCacheController {
//	    
//    	//private static final Logger log = (Logger) LoggerFactory.getLogger(InsertManualCacheController.class);
//	    @Autowired
//	    private HazelcastInstance hazelcastInstance;
//	    
//	    private final ObjectMapper mapper = new ObjectMapper();
//	    
//	    @PostMapping("/add-cache")
//	    public String addToCache() {
//	        try {
//	            IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
//	            
//	            Map<String, Object> customerMap = new HashMap<>();
//	            customerMap.put("customerId", 999);
//	            customerMap.put("customerCode", "TEST999");
//	            customerMap.put("customerName", "Test Manual");
//	            customerMap.put("email", "test@manual.com");
//	            customerMap.put("city", "Jakarta");
//	            customerMap.put("createdDate", LocalDateTime.now().toString());
//	            customerMap.put("updatedDate", LocalDateTime.now().toString());
//	            customerMap.put("source", "MANUAL");
//	            
//	            String json = mapper.writeValueAsString(customerMap);
//	            HazelcastJsonValue jsonValue = new HazelcastJsonValue(json);
//	            
//	            cache.put(999, jsonValue);
//	            
//	            return "Data added to cache as JSON";
//	            
//	        } catch (Exception e) {
//	            return "Error: " + e.getMessage();
//	        }
//	    }
//	    
//	    @DeleteMapping("/clear-cache")
//	    public String clearCache() {
//	        IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
//	        cache.clear();
//	        return "Cache cleared";
//	    }
//	    
//	    
//	    @DeleteMapping("/cache")
//	    public ResponseEntity<Map<String, String>> clearAllCache() {
//	        log.info("Clearing all cache");
//	    	System.out.println("Clearing all cache");
//	        IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
//	        cache.clear();
//	        return ResponseEntity.ok(Map.of("message", "All cache cleared"));
//	    }
//	    
//	    @DeleteMapping("/clear-customers")
//	    public String clearCustomers() {
//	        IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
//	        int size = cache.size();
//	        cache.clear();
//	        return String.format("Cleared %d items from customers map", size);
//	    }
//	    
//	    @GetMapping("/cache/keys")
//	    public ResponseEntity<List<Integer>> getCacheKeys() {
//	        IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
//	        Set<Integer> keys = cache.keySet();
//	        log.info("Cache keys: {}", keys);
//	       // System.out.println("Cache keys:"+keys);
//	        return ResponseEntity.ok(new ArrayList<>(keys));
//	    }
//	}