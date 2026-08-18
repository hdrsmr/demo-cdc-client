package com.abc.hazelcast.cdc.service.impl;



import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.abc.hazelcast.cdc.dto.CustomerRequest;
import com.abc.hazelcast.cdc.dto.CustomerResponse;
import com.abc.hazelcast.cdc.dto.PageResponse;
import com.abc.hazelcast.cdc.entity.Customer;
import com.abc.hazelcast.cdc.exception.ResourceAlreadyExistsException;
import com.abc.hazelcast.cdc.exception.ResourceNotFoundException;
import com.abc.hazelcast.cdc.repository.CustomerRepository;
import com.abc.hazelcast.cdc.service.CustomerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class CustomerServiceImpl implements CustomerService {

	private static final String CACHE_NAME = "customers";
	
    private final CustomerRepository customerRepository;
    private final HazelcastInstance hazelcastInstance;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional
    public CustomerResponse createCustomer(CustomerRequest request) {
        log.info("Creating new customer with code: {}", request.getCustomerCode());

        // Check if customer code already exists
        if (customerRepository.existsByCustomerCode(request.getCustomerCode())) {
            throw new ResourceAlreadyExistsException(
                "Customer with code '" + request.getCustomerCode() + "' already exists"
            );
        }

        // Map request to entity
        Customer customer = new Customer();
        customer.setCustomerCode(request.getCustomerCode());
        customer.setCustomerName(request.getCustomerName());
        customer.setEmail(request.getEmail());
        customer.setCity(request.getCity());

        // Save to database
        Customer savedCustomer = customerRepository.save(customer);
        log.info(" Customer created successfully with ID: {}", savedCustomer.getCustomerId());

        //  Tambahkan ke cache
        addCustomerToCache(savedCustomer);

        return mapToResponse(savedCustomer);
    }

    @Override
    public CustomerResponse getCustomerById(Integer id) {
        log.info("Fetching customer by ID: {}", id);
        
        //  Coba ambil dari cache dulu
        CustomerResponse cachedCustomer = getCustomerFromCache(id);
        if (cachedCustomer != null) {
            log.info(" Customer found in cache: ID={}", id);
            return cachedCustomer;
        }
        
        // Jika tidak ada di cache, ambil dari database
        Customer customer = customerRepository.findById(id)
            .orElseThrow(() -> new RuntimeException("Customer not found with ID: " + id));
        
        log.info(" Customer found in database: ID={}", id);
        
        // Tambahkan ke cache untuk下次 akses
        addCustomerToCache(customer);
        
        return mapToResponse(customer);
    }


    /**
     *  Tambahkan customer ke Hazelcast cache dengan konfigurasi LRU & TTL
     */
    private void addCustomerToCache(Customer customer) {
        try {
            IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
            
            Map<String, Object> customerMap = new HashMap<>();
            customerMap.put("customerId", customer.getCustomerId());
            customerMap.put("customerCode", customer.getCustomerCode());
            customerMap.put("customerName", customer.getCustomerName());
            customerMap.put("email", customer.getEmail());
            customerMap.put("city", customer.getCity());
            customerMap.put("createdDate", customer.getCreatedDate().toString());
            customerMap.put("updatedDate", customer.getUpdatedDate() != null ? 
                customer.getUpdatedDate().toString() : null);
            customerMap.put("source", "DATABASE_CREATED");
            
            String json = objectMapper.writeValueAsString(customerMap);
            HazelcastJsonValue jsonValue = new HazelcastJsonValue(json);
            
            //  TTL 1 jam - sesuai konfigurasi di HazelcastConfig
            cache.put(customer.getCustomerId(), jsonValue, 1, TimeUnit.HOURS);
            
            log.info(" Customer added to Hazelcast cache: ID={}, TTL=1 hour", 
                customer.getCustomerId());
            
        } catch (Exception e) {
            log.error(" Failed to add customer to cache: {}", e.getMessage());
        }
    }

    /**
     *  Ambil customer dari cache
     */
    private CustomerResponse getCustomerFromCache(Integer customerId) {
        try {
            IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
            HazelcastJsonValue value = cache.get(customerId);
            
            if (value == null) {
                return null;
            }
            
            return objectMapper.readValue(value.toString(), CustomerResponse.class);
            
        } catch (Exception e) {
            log.error(" Failed to get customer from cache: {}", e.getMessage());
            return null;
        }
    }

    private CustomerResponse mapToResponse(Customer customer) {
        CustomerResponse response = new CustomerResponse();
        response.setCustomerId(customer.getCustomerId());
        response.setCustomerCode(customer.getCustomerCode());
        response.setCustomerName(customer.getCustomerName());
        response.setEmail(customer.getEmail());
        response.setCity(customer.getCity());
        response.setCreatedDate(customer.getCreatedDate());
        response.setUpdatedDate(customer.getUpdatedDate());
        response.setSource("DATABASE");
        return response;
    }
    
  @Override
  public void evictCache(String cacheKey) {
      log.info("Evicting cache for key: {}", cacheKey);
      
      IMap<Object, Object> cache = hazelcastInstance.getMap(CACHE_NAME);
      if (cacheKey != null) {
          cache.remove(cacheKey);
      }
  }
  
  
//============================================
  // UPDATE
  // ============================================

  @Override
  @Transactional
  public CustomerResponse updateCustomerMulti2(Integer customerId, Map<String, Object> updates) {
      log.info("Updating customer ID: {} with fields: {}", customerId, updates);
      
      // 1. Update database
      Customer customer = customerRepository.findById(customerId)
          .orElseThrow(() -> new RuntimeException("Customer not found with ID: " + customerId));
      
      if (updates.containsKey("customerName")) {
          customer.setCustomerName((String) updates.get("customerName"));
      }
      if (updates.containsKey("customerCode")) {
          customer.setCustomerCode((String) updates.get("customerCode"));
      }
      if (updates.containsKey("email")) {
          customer.setEmail((String) updates.get("email"));
      }
      if (updates.containsKey("city")) {
          customer.setCity((String) updates.get("city"));
      }
      
      Customer saved = customerRepository.save(customer);
      log.info(" Database updated for customer ID: {}", customerId);
      
      //  Update cache secara manual dengan TTL & LRU
      updateCustomerCache(saved);
      
      return CustomerResponse.fromEntity(saved, "DATABASE_UPDATED");
  }

  // ============================================
  // GET FROM CACHE
  // ============================================

  @Override
  public List<CustomerResponse> getAllFromCache() {
      log.info("Fetching all customers from cache");
      
      IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
      List<CustomerResponse> customers = new ArrayList<>();
      
      cache.forEach((key, value) -> {
          try {
              CustomerResponse response = objectMapper.readValue(value.toString(), CustomerResponse.class);
              customers.add(response);
          } catch (Exception e) {
              log.error("Error parsing customer from cache: {}", e.getMessage());
          }
      });
      
      log.info("Found {} customers in cache", customers.size());
      return customers;
  }

  // ============================================
  // PRIVATE METHODS
  // ============================================

  /**
   *  Tambahkan customer ke Hazelcast cache
   */
  @SuppressWarnings("unused")
  private void addCustomerToCache(Customer customer, String source) {
      try {
          IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
          
          Map<String, Object> customerMap = buildCustomerMap(customer, source);
          
          String json = objectMapper.writeValueAsString(customerMap);
          HazelcastJsonValue jsonValue = new HazelcastJsonValue(json);
          
          //  TTL 1 jam
          cache.put(customer.getCustomerId(), jsonValue, 1, TimeUnit.HOURS);
          
          log.info(" Customer added to cache: ID={}, TTL=1 hour, LRU enabled", 
              customer.getCustomerId());
          
      } catch (Exception e) {
          log.error(" Failed to add customer to cache: {}", e.getMessage());
      }
  }

  /**
   *  Update customer di cache (reset TTL)
   */
  private void updateCustomerCache(Customer customer) {
      try {
          IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
          
          boolean exists = cache.containsKey(customer.getCustomerId());
          
          Map<String, Object> customerMap = buildCustomerMap(customer, "CACHE_UPDATED_MANUAL");
          
          String json = objectMapper.writeValueAsString(customerMap);
          HazelcastJsonValue jsonValue = new HazelcastJsonValue(json);
          
          //  Update cache dengan TTL (reset timer)
          cache.put(customer.getCustomerId(), jsonValue, 1, TimeUnit.HOURS);
          
          log.info(" Customer cache updated: ID={}, TTL reset, existed: {}", 
              customer.getCustomerId(), exists);
          
      } catch (Exception e) {
          log.error(" Failed to update customer cache: {}", e.getMessage());
      }
  }
  
  @Override
@Transactional(readOnly = true)
public PageResponse<CustomerResponse> getAllCustomers(int page, int size, String sortBy, String sortDirection) {
    log.info("Fetching all customers - page: {}, size: {}, sortBy: {}, direction: {}", 
             page, size, sortBy, sortDirection);

    Sort sort = Sort.by(Sort.Direction.fromString(sortDirection), sortBy);
    Pageable pageable = PageRequest.of(page, size, sort);
    Page<Customer> customerPage = customerRepository.findAll(pageable);

    return mapToPageResponse(customerPage);
}
  

  /**
   *  Build Map untuk cache
   */
  private Map<String, Object> buildCustomerMap(Customer customer, String source) {
      Map<String, Object> customerMap = new HashMap<>();
      customerMap.put("customerId", customer.getCustomerId());
      customerMap.put("customerCode", customer.getCustomerCode());
      customerMap.put("customerName", customer.getCustomerName());
      customerMap.put("email", customer.getEmail());
      customerMap.put("city", customer.getCity());
      customerMap.put("createdDate", customer.getCreatedDate().toString());
      customerMap.put("updatedDate", LocalDateTime.now().toString());
      customerMap.put("source", source);
      return customerMap;
  }
  
  // Helper method to map page to page response
  private PageResponse<CustomerResponse> mapToPageResponse(Page<Customer> customerPage) {
      List<CustomerResponse> content = customerPage.getContent()
          .stream()
          .map(this::mapToResponse)
          .collect(Collectors.toList());

      return new PageResponse<>(
          content,
          customerPage.getNumber(),
          customerPage.getSize(),
          customerPage.getTotalElements(),
          customerPage.getTotalPages(),
          customerPage.isLast()
      );
  }

  
  
  // ============================================
  // DELETE 
  // ============================================

  @Override
  @Transactional
  public void deleteCustomer(Integer id) {
      log.info("Deleting customer with ID: {}", id);
      
      // 1. Cek apakah customer ada
      Customer customer = findCustomerById(id);
      
      // 2. Hapus dari database
      customerRepository.delete(customer);
      log.info(" Customer deleted from database: ID={}", id);
      
      // 3.  Hapus dari cache
      removeCustomerFromCache(id);
  }

	private Customer findCustomerById(Integer id) {
	return customerRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException(
	      "Customer with ID '" + id + "' not found"
	  ));
	}

/**
 *  Hapus customer dari cache
 */
private void removeCustomerFromCache(Integer customerId) {
    try {
        // 1. Hapus dari customers map
        IMap<Integer, HazelcastJsonValue> customerCache = hazelcastInstance.getMap("customers");
        
        boolean existed = customerCache.containsKey(customerId);
        if (existed) {
            customerCache.remove(customerId);
            log.info(" Customer removed from customers cache: ID={}", customerId);
        } else {
            log.info("⚠️ Customer not found in customers cache: ID={}", customerId);
        }
        
        // 2. Hapus dari temp-cache (jika ada)
        removeFromTempCacheByCustomerId(customerId);
        
    } catch (Exception e) {
        log.error(" Failed to remove customer from cache: {}", e.getMessage());
    }
}

/**
 *  Hapus customer dari temp-cache
 */
	private void removeFromTempCacheByCustomerId(Integer customerId) {
	    try {
	        IMap<String, String> tempCache = hazelcastInstance.getMap("temp-cache");
	        List<String> keysToRemove = new ArrayList<>();
	        
	        for (Map.Entry<String, String> entry : tempCache.entrySet()) {
	            try {
	                Map<String, Object> value = objectMapper.readValue(entry.getValue(), Map.class);
	                Integer cachedCustomerId = (Integer) value.get("customerId");
	                if (cachedCustomerId != null && cachedCustomerId.equals(customerId)) {
	                    keysToRemove.add(entry.getKey());
	                }
	            } catch (Exception e) {
	                // Skip entry yang tidak bisa di-parse
	            }
	        }
	        
	        for (String key : keysToRemove) {
	            tempCache.remove(key);
	            log.info(" Customer removed from temp-cache: key={}", key);
	        }
	        
	    } catch (Exception e) {
	        log.error(" Failed to remove from temp-cache: {}", e.getMessage());
	    }
	}


}

	

//
//
//import com.abc.hazelcast.cdc.dto.CustomerRequest;
//import com.abc.hazelcast.cdc.dto.CustomerResponse;
//import com.abc.hazelcast.cdc.dto.PageResponse;
//import com.abc.hazelcast.cdc.entity.Customer;
//import com.abc.hazelcast.cdc.exception.ResourceAlreadyExistsException;
////import com.abc.hazelcast.cdc.exception.ResourceAlreadyExistsException;
//import com.abc.hazelcast.cdc.exception.ResourceNotFoundException;
//import com.abc.hazelcast.cdc.processor.UpdateCustomerEntryProcessor;
//import com.abc.hazelcast.cdc.repository.CustomerRepository;
//import com.abc.hazelcast.cdc.service.CustomerService;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.core.HazelcastJsonValue;
//import com.hazelcast.map.IMap;
////import com.hazelcast.shaded.com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.domain.Page;
//import org.springframework.data.domain.PageRequest;
//import org.springframework.data.domain.Pageable;
//import org.springframework.data.domain.Sort;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.time.LocalDateTime;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
//@Service
//@RequiredArgsConstructor
//@Slf4j
//@Transactional
//public class CustomerServiceImpl implements CustomerService {
//
//    private final CustomerRepository customerRepository;
//    
//    private final HazelcastInstance hazelcastInstance;
//
//    private static final String CACHE_NAME = "customers";
//    
//   // private final ObjectMapper mapper = new ObjectMapper();
//    @Autowired
//    private ObjectMapper objectMapper;
//
//
//    @Override
//    public CustomerResponse createCustomer(CustomerRequest request) {
//        log.info("Creating new customer with code: {}", request.getCustomerCode());
//
//        // Check if customer code already exists
//        if (customerRepository.existsByCustomerCode(request.getCustomerCode())) {
//            throw new ResourceAlreadyExistsException(
//                "Customer with code '" + request.getCustomerCode() + "' already exists"
//            );
//        }
//
//        // Map request to entity
//        Customer customer = new Customer();
//        customer.setCustomerCode(request.getCustomerCode());
//        customer.setCustomerName(request.getCustomerName());
//        customer.setEmail(request.getEmail());
//        customer.setCity(request.getCity());
//
//        // Save to database
//        Customer savedCustomer = customerRepository.save(customer);
//        log.info("Customer created successfully with ID: {}", savedCustomer.getCustomerId());
//
//        return mapToResponse(savedCustomer);
//    }
//
//    @Override
//    @Transactional(readOnly = true)
//    public CustomerResponse getCustomerById(Integer id) {
//        log.info("Fetching customer by ID: {}", id);
//        Customer customer = findCustomerById(id);
//        return mapToResponse(customer);
//    }
//
//    @Override
//    @Transactional(readOnly = true)
//    public CustomerResponse getCustomerByCode(String customerCode) {
//        log.info("Fetching customer by code: {}", customerCode);
//        Customer customer = customerRepository.findByCustomerCode(customerCode)
//            .orElseThrow(() -> new ResourceNotFoundException(
//                "Customer with code '" + customerCode + "' not found"
//            ));
//        return mapToResponse(customer);
//    }
//
//    @Override
//    @Transactional(readOnly = true)
//    public PageResponse<CustomerResponse> getAllCustomers(int page, int size, String sortBy, String sortDirection) {
//        log.info("Fetching all customers - page: {}, size: {}, sortBy: {}, direction: {}", 
//                 page, size, sortBy, sortDirection);
//
//        Sort sort = Sort.by(Sort.Direction.fromString(sortDirection), sortBy);
//        Pageable pageable = PageRequest.of(page, size, sort);
//        Page<Customer> customerPage = customerRepository.findAll(pageable);
//
//        return mapToPageResponse(customerPage);
//    }
//
//    @Override
//    @Transactional(readOnly = true)
//    public PageResponse<CustomerResponse> searchCustomers(String keyword, int page, int size, 
//                                                          String sortBy, String sortDirection) {
//        log.info("Searching customers with keyword: {}", keyword);
//
//        Sort sort = Sort.by(Sort.Direction.fromString(sortDirection), sortBy);
//        Pageable pageable = PageRequest.of(page, size, sort);
//        Page<Customer> customerPage = customerRepository.searchCustomers(keyword, pageable);
//
//        return mapToPageResponse(customerPage);
//    }
//
////    @Override
////    public CustomerResponse updateCustomer(Integer id, CustomerRequest request) {
////        log.info("Updating customer with ID: {}", id);
////
////        // Find existing customer
////        Customer customer = findCustomerById(id);
////
////        // Check if customer code is being changed and if new code already exists
////        if (!customer.getCustomerCode().equals(request.getCustomerCode()) &&
////            customerRepository.existsByCustomerCodeAndIdNot(request.getCustomerCode(), id)) {
////            throw new ResourceAlreadyExistsException(
////                "Customer with code '" + request.getCustomerCode() + "' already exists"
////            );
////        }
////
////        // Update fields
////        customer.setCustomerCode(request.getCustomerCode());
////        customer.setCustomerName(request.getCustomerName());
////        customer.setEmail(request.getEmail());
////        customer.setCity(request.getCity());
////
////        // Save updated customer
////        Customer updatedCustomer = customerRepository.save(customer);
////        log.info("Customer updated successfully with ID: {}", updatedCustomer.getCustomerId());
////
////        return mapToResponse(updatedCustomer);
////    }
//
//    @Override
//    public void deleteCustomer(Integer id) {
//        log.info("Deleting customer with ID: {}", id);
//        Customer customer = findCustomerById(id);
//        customerRepository.delete(customer);
//        log.info("Customer deleted successfully with ID: {}", id);
//    }
//
//    @Override
//    @Transactional(readOnly = true)
//    public boolean existsByCustomerCode(String customerCode) {
//        return customerRepository.existsByCustomerCode(customerCode);
//    }
//
//    // Helper method to find customer by ID
//    private Customer findCustomerById(Integer id) {
//        return customerRepository.findById(id)
//            .orElseThrow(() -> new ResourceNotFoundException(
//                "Customer with ID '" + id + "' not found"
//            ));
//    }
//
//    // Helper method to map entity to response DTO
//    private CustomerResponse mapToResponse(Customer customer) {
//        CustomerResponse response = new CustomerResponse();
//        response.setCustomerId(customer.getCustomerId());
//        response.setCustomerCode(customer.getCustomerCode());
//        response.setCustomerName(customer.getCustomerName());
//        response.setEmail(customer.getEmail());
//        response.setCity(customer.getCity());
////        response.setCreatedDate(customer.getCreatedDate());
////        response.setUpdatedDate(customer.getUpdatedDate());
//        return response;
//    }
//
//    // Helper method to map page to page response
//    private PageResponse<CustomerResponse> mapToPageResponse(Page<Customer> customerPage) {
//        List<CustomerResponse> content = customerPage.getContent()
//            .stream()
//            .map(this::mapToResponse)
//            .collect(Collectors.toList());
//
//        return new PageResponse<>(
//            content,
//            customerPage.getNumber(),
//            customerPage.getSize(),
//            customerPage.getTotalElements(),
//            customerPage.getTotalPages(),
//            customerPage.isLast()
//        );
//    }
//
//    @Override
//    public void evictCache(String cacheKey) {
//        log.info("Evicting cache for key: {}", cacheKey);
//        
//        IMap<Object, Object> cache = hazelcastInstance.getMap(CACHE_NAME);
//        if (cacheKey != null) {
//            cache.remove(cacheKey);
//        }
//    }
//
//
//    
//    @Override
//    @Transactional
//    public CustomerResponse updateEmail(String customerCode, String newEmail) {
//        log.info("Updating email for customer code: {} to {}", customerCode, newEmail);
//        
//        int rowsAffected = customerRepository.updateEmailByCustomerCode(customerCode, newEmail);
//        
//        if (rowsAffected == 0) {
//            throw new ResourceNotFoundException("Customer with code '" + customerCode + "' not found");
//        }
//        
//        // Ambil data yang sudah diupdate
//        Customer updatedCustomer = customerRepository.findByCustomerCode(customerCode)
//            .orElseThrow(() -> new ResourceNotFoundException("Customer not found after update"));
//        
//        return mapToResponse(updatedCustomer);
//    }
//    
////    @Override
////    @Transactional
////    public CustomerResponse updateEmailById(Integer customerId, String newEmail) {
////        log.info("Updating email for customer ID: {} to {}", customerId, newEmail);
////        
////        Customer customer = customerRepository.findById(customerId)
////            .orElseThrow(() -> new ResourceNotFoundException("Customer with ID " + customerId + " not found"));
////        
////        // Update email langsung tanpa stored procedure
////        customer.setEmail(newEmail);
////        customer.setUpdatedDate(LocalDateTime.now());
////        
////        Customer updatedCustomer = customerRepository.save(customer);
////        return mapToResponse(updatedCustomer);
////    }
//    
//    
//    //new jalan
//    @Override
//    public CustomerResponse updateEmailById(Integer customerId, String newEmail) {
//        log.info("Updating email for customer ID: {} to {}", customerId, newEmail);
//        
//        //  Buat Map untuk updates
//        Map<String, Object> updates = new HashMap<>();
//        updates.put("email", newEmail);
//        
//        //  Buat instance baru langsung (bukan Spring Bean)
//        UpdateCustomerEntryProcessor processor = new UpdateCustomerEntryProcessor(updates, objectMapper);
//        
//        IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
//        
//        //  Execute entry processor
//        CustomerResponse updated = cache.executeOnKey(customerId, processor);
//        
//        if (updated == null) {
//            throw new RuntimeException("Customer not found with ID: " + customerId);
//        }
//        
//        log.info(" Email updated for customer ID: {} via Entry Processor", customerId);
//        return updated;
//    }
//
//    
//    
//    //
//    @Override
//    @Transactional
//    public CustomerResponse updateCustomerMulti2(Integer customerId, Map<String, Object> updates) {
//        log.info("Updating customer ID: {} with fields: {}", customerId, updates);
//        
//        // ONLY update database
//        Customer customer = customerRepository.findById(customerId)
//            .orElseThrow(() -> new RuntimeException("Customer not found with ID: " + customerId));
//        
//        if (updates.containsKey("customerName")) {
//            customer.setCustomerName((String) updates.get("customerName"));
//        }
//        if (updates.containsKey("customerCode")) {
//            customer.setCustomerCode((String) updates.get("customerCode"));
//        }
//        if (updates.containsKey("email")) {
//            customer.setEmail((String) updates.get("email"));
//        }
//        if (updates.containsKey("city")) {
//            customer.setCity((String) updates.get("city"));
//        }
//        
//        Customer saved = customerRepository.save(customer);
//        log.info("Database updated for customer ID: {}", customerId);
//        
//        // CDC akan otomatis mengupdate cache!
//        // Debezium akan menangkap perubahan dan memanggil handleUpdate
//        
//        return CustomerResponse.fromEntity(saved, "DATABASE_UPDATED");
//    }
//    
//    //
// 
//    
//    
//    @Override
//    public CustomerResponse updateCustomerMulti(Integer customerId, Map<String, Object> updates) {
//        log.info("Updating customer ID: {} with fields: {}", customerId, updates);
//        
//        IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
//        
//        //  Cek apakah data ada
//        if (!cache.containsKey(customerId)) {
//            log.warn("Customer not found with ID: {}, creating new entry", customerId);
//            return createCustomerFromUpdates(customerId, updates);
//        }
//        
//        UpdateCustomerEntryProcessor processor = new UpdateCustomerEntryProcessor(updates, objectMapper);
//        CustomerResponse response = cache.executeOnKey(customerId, processor);
//        
//        if (response == null) {
//            throw new RuntimeException("Customer not found with ID: " + customerId);
//        }
//        
//        return response;
//    }
//
//    // Method untuk create customer jika tidak ada
//    private CustomerResponse createCustomerFromUpdates(Integer customerId, Map<String, Object> updates) {
//        try {
//            Map<String, Object> customerMap = new HashMap<>();
//            customerMap.put("customerId", customerId);
//            customerMap.put("customerCode", updates.getOrDefault("customerCode", "CUST" + customerId));
//            customerMap.put("customerName", updates.getOrDefault("customerName", "Customer " + customerId));
//            customerMap.put("email", updates.getOrDefault("email", "customer" + customerId + "@test.com"));
//            customerMap.put("city", updates.getOrDefault("city", "Jakarta"));
//            customerMap.put("createdDate", LocalDateTime.now().toString());
//            customerMap.put("updatedDate", LocalDateTime.now().toString());
//            customerMap.put("source", "HAZELCAST_CREATED");
//            
//            String json = objectMapper.writeValueAsString(customerMap);
//            HazelcastJsonValue jsonValue = new HazelcastJsonValue(json);
//            
//            IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
//            cache.put(customerId, jsonValue);
//            
//            CustomerResponse response = parseCustomerResponse(customerMap);
//            log.info(" New customer created in cache: ID={}", customerId);
//            
//            return response;
//            
//        } catch (Exception e) {
//            log.error("Failed to create customer: {}", e.getMessage());
//            throw new RuntimeException("Failed to create customer: " + e.getMessage());
//        }
//    }
//    
//
//    
//    private CustomerResponse parseCustomerResponse(Map<String, Object> map) {
//        CustomerResponse response = new CustomerResponse();
//        response.setCustomerId((Integer) map.get("customerId"));
//        response.setCustomerCode((String) map.get("customerCode"));
//        response.setCustomerName((String) map.get("customerName"));
//        response.setEmail((String) map.get("email"));
//        response.setCity((String) map.get("city"));
////        response.setCreatedDate((String) map.get("createdDate"));
////        response.setUpdatedDate((String) map.get("updatedDate"));
//        response.setCreatedDate((LocalDateTime) map.get("createdDate"));
//        response.setUpdatedDate( (LocalDateTime) map.get("updatedDate"));
//        response.setSource((String) map.get("source"));
//        return response;
//    }
//
//
//	public void checkCache() {
//        IMap<Integer, CustomerResponse> cache = hazelcastInstance.getMap("customers");
//        log.info("Cache size: {}", cache.size());
//        cache.forEach((key, value) -> {
//            log.info("Key: {}, Value: {}", key, value);
//        });
//    }
//    
//
//    @Override
//    public List<CustomerResponse> getAllFromCache() {
//        log.info("Fetching all customers from cache");
//        
//        IMap<Integer, HazelcastJsonValue> cache = hazelcastInstance.getMap("customers");
//        List<CustomerResponse> customers = new ArrayList<>();
//        
//        cache.forEach((key, value) -> {
//            try {
//                CustomerResponse response = objectMapper.readValue(value.toString(), CustomerResponse.class);
//                customers.add(response);
//            } catch (Exception e) {
//                log.error("Error parsing customer from cache: {}", e.getMessage());
//            }
//        });
//        
//        return customers;
//    }
//
//	
//}
//
//
