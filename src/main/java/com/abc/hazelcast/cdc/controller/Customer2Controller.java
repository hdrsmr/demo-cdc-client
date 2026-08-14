package com.abc.hazelcast.cdc.controller;


import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.abc.hazelcast.cdc.dto.CustomerRequest;
import com.abc.hazelcast.cdc.dto.CustomerResponse;
import com.abc.hazelcast.cdc.dto.PageResponse;
import com.abc.hazelcast.cdc.service.CustomerService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/v1/customers")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Customer Management", description = "APIs for managing customers")
public class Customer2Controller {

    private final CustomerService customerService;
   

    @Operation(summary = "Create a new customer", description = "Create a new customer with the provided details")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "201", description = "Customer created successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid input"),
        @ApiResponse(responseCode = "409", description = "Customer code already exists")
    })
    @PostMapping
    public ResponseEntity<CustomerResponse> createCustomer(@Valid @RequestBody CustomerRequest request) {
        log.info("Received request to create customer with code: {}", request.getCustomerCode());
        CustomerResponse response = customerService.createCustomer(request);
        return new ResponseEntity<>(response, HttpStatus.CREATED);
    }

    @Operation(summary = "Get customer by ID", description = "Retrieve customer details by ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Customer found"),
        @ApiResponse(responseCode = "404", description = "Customer not found")
    })
    @GetMapping("/{id}")
    public ResponseEntity<CustomerResponse> getCustomerById(@PathVariable Integer id) {
        log.info("Received request to get customer by ID: {}", id);
        CustomerResponse response = customerService.getCustomerById(id);
        return ResponseEntity.ok(response);
    }
    
    
    //add
  @Operation(summary = "Get all customers from cache")
  @GetMapping("/cache")
  public ResponseEntity<List<CustomerResponse>> getAllFromCache() {
      log.info("GET /api/v1/customers/cache - Getting all customers from cache");
      List<CustomerResponse> customers = customerService.getAllFromCache();
      return ResponseEntity.ok(customers);
  }
  
  @PutMapping("/{customerId}")
  @Operation(summary = "Update customer Multi")
  public ResponseEntity<CustomerResponse> updateCustomerMulti(
          @Parameter(description = "Customer ID", example = "1") 
          @PathVariable Integer customerId,
          @Valid @RequestBody Map<String, Object> request) {
      
      log.info("PUT /api/v1/customers/{} - Updating customer", customerId);
      CustomerResponse response = customerService.updateCustomerMulti2(customerId, request);
      return ResponseEntity.ok(response);
  }
  
	@Operation(summary = "Get all customers", description = "Retrieve all customers with pagination and sorting")
	@GetMapping
	public ResponseEntity<PageResponse<CustomerResponse>> getAllCustomers(
	        @Parameter(description = "Page number (0-based)") @RequestParam(defaultValue = "0") int page,
	        @Parameter(description = "Page size") @RequestParam(defaultValue = "10") int size,
	        @Parameter(description = "Sort by field") @RequestParam(defaultValue = "customerId") String sortBy,
	        @Parameter(description = "Sort direction (ASC/DESC)") @RequestParam(defaultValue = "ASC") String sortDirection) {
	    log.info("Received request to get all customers - page: {}, size: {}, sortBy: {}, direction: {}", 
	             page, size, sortBy, sortDirection);
	    PageResponse<CustomerResponse> response = customerService.getAllCustomers(page, size, sortBy, sortDirection);
	    return ResponseEntity.ok(response);
	}

	//Delete
	@Operation(summary = "Delete customer", description = "Delete a customer by ID")
	@ApiResponses(value = {
	  @ApiResponse(responseCode = "204", description = "Customer deleted successfully"),
	  @ApiResponse(responseCode = "404", description = "Customer not found")
	})
	@DeleteMapping("/{id}")
	public ResponseEntity<Void> deleteCustomer(@PathVariable Integer id) {
	  log.info("Received request to delete customer with ID: {}", id);
	  customerService.deleteCustomer(id);
	  return ResponseEntity.noContent().build();
	}
  
  

//    @Operation(summary = "Get customer by code", description = "Retrieve customer details by customer code")
//    @ApiResponses(value = {
//        @ApiResponse(responseCode = "200", description = "Customer found"),
//        @ApiResponse(responseCode = "404", description = "Customer not found")
//    })
//    @GetMapping("/code/{customerCode}")
//    public ResponseEntity<CustomerResponse> getCustomerByCode(@PathVariable String customerCode) {
//        log.info("Received request to get customer by code: {}", customerCode);
//        CustomerResponse response = customerService.getCustomerByCode(customerCode);
//        return ResponseEntity.ok(response);
//    }
//
//    @Operation(summary = "Get all customers", description = "Retrieve all customers with pagination and sorting")
//    @GetMapping
//    public ResponseEntity<PageResponse<CustomerResponse>> getAllCustomers(
//            @Parameter(description = "Page number (0-based)") @RequestParam(defaultValue = "0") int page,
//            @Parameter(description = "Page size") @RequestParam(defaultValue = "10") int size,
//            @Parameter(description = "Sort by field") @RequestParam(defaultValue = "customerId") String sortBy,
//            @Parameter(description = "Sort direction (ASC/DESC)") @RequestParam(defaultValue = "ASC") String sortDirection) {
//        log.info("Received request to get all customers - page: {}, size: {}, sortBy: {}, direction: {}", 
//                 page, size, sortBy, sortDirection);
//        PageResponse<CustomerResponse> response = customerService.getAllCustomers(page, size, sortBy, sortDirection);
//        return ResponseEntity.ok(response);
//    }
//
//    @Operation(summary = "Search customers", description = "Search customers by keyword (name, city, or email)")
//    @GetMapping("/search")
//    public ResponseEntity<PageResponse<CustomerResponse>> searchCustomers(
//            @Parameter(description = "Search keyword") @RequestParam String keyword,
//            @Parameter(description = "Page number (0-based)") @RequestParam(defaultValue = "0") int page,
//            @Parameter(description = "Page size") @RequestParam(defaultValue = "10") int size,
//            @Parameter(description = "Sort by field") @RequestParam(defaultValue = "customerId") String sortBy,
//            @Parameter(description = "Sort direction (ASC/DESC)") @RequestParam(defaultValue = "ASC") String sortDirection) {
//        log.info("Received request to search customers with keyword: {}", keyword);
//        PageResponse<CustomerResponse> response = customerService.searchCustomers(keyword, page, size, sortBy, sortDirection);
//        return ResponseEntity.ok(response);
//    }


//    @Operation(summary = "Update customer email by customer code", 
//            description = "Update email address using customer code")
// @ApiResponses(value = {
//     @ApiResponse(responseCode = "200", description = "Email updated successfully"),
//     @ApiResponse(responseCode = "400", description = "Invalid email format"),
//     @ApiResponse(responseCode = "404", description = "Customer not found")
// })
// @PutMapping("/{customerCode}/email")
// public ResponseEntity<CustomerResponse> updateCustomerEmail(
//         @Parameter(description = "Customer code", example = "CUST001")
//         @PathVariable String customerCode,
//         
//         @Parameter(description = "New email address", example = "newemail@test.com")
//         @RequestParam String newEmail) {
//     
//     log.info("Updating email for customer code: {} to {}", customerCode, newEmail);
//     CustomerResponse response = customerService.updateEmail(customerCode, newEmail);
//     return ResponseEntity.ok(response);
// }
//
//
//    
//    @PutMapping("/{customerId}/email")
//    public ResponseEntity<CustomerResponse> updateCustomerEmailById(
//            @PathVariable Integer customerId,
//            @RequestParam String newEmail) {
//        
//        log.info("Updating email for customer ID: {} to {}", customerId, newEmail);
//        CustomerResponse response = customerService.updateEmailById(customerId, newEmail);
//        return ResponseEntity.ok(response);
//    }
//
//    @Operation(summary = "Delete customer", description = "Delete a customer by ID")
//    @ApiResponses(value = {
//        @ApiResponse(responseCode = "204", description = "Customer deleted successfully"),
//        @ApiResponse(responseCode = "404", description = "Customer not found")
//    })
//    @DeleteMapping("/{id}")
//    public ResponseEntity<Void> deleteCustomer(@PathVariable Integer id) {
//        log.info("Received request to delete customer with ID: {}", id);
//        customerService.deleteCustomer(id);
//        return ResponseEntity.noContent().build();
//    }
//
//    @Operation(summary = "Check if customer code exists", description = "Check if a customer code already exists")
//    @GetMapping("/exists/{customerCode}")
//    public ResponseEntity<Boolean> existsByCustomerCode(@PathVariable String customerCode) {
//        log.info("Received request to check customer code existence: {}", customerCode);
//        boolean exists = customerService.existsByCustomerCode(customerCode);
//        return ResponseEntity.ok(exists);
//    }
    
    //add
//    @Operation(summary = "Get all customers from cache")
//    @GetMapping("/cache")
//    public ResponseEntity<List<CustomerResponse>> getAllFromCache() {
//        log.info("GET /api/v1/customers/cache - Getting all customers from cache");
//        List<CustomerResponse> customers = customerService.getAllFromCache();
//        return ResponseEntity.ok(customers);
//    }
//
//    @Operation(summary = "Evict specific cache key")
//    @DeleteMapping("/cache/{key}")
//    public ResponseEntity<Map<String, String>> evictCache(
//            @Parameter(description = "Cache key to evict") @PathVariable String key) {
//        log.info("DELETE /api/v1/customers/cache/{}", key);
//        customerService.evictCache(key);
//        return ResponseEntity.ok(Map.of("message", "Cache evicted for key: " + key));
//    }
//
//    @Operation(summary = "Clear all cache")
//    @DeleteMapping("/cache")
//    public ResponseEntity<Map<String, String>> clearAllCache() {
//        log.info("DELETE /api/v1/customers/cache - Clearing all cache");
//        customerService.evictCache(null);
//        return ResponseEntity.ok(Map.of("message", "All cache cleared"));
//    }
    
    
    //add
    
    
//    @PutMapping("/{customerId}")
//    @Operation(summary = "Update customer Multi")
//    public ResponseEntity<CustomerResponse> updateCustomerMulti(
//            @Parameter(description = "Customer ID", example = "1") 
//            @PathVariable Integer customerId,
//            @Valid @RequestBody Map<String, Object> request) {
//        
//        log.info("PUT /api/v1/customers/{} - Updating customer", customerId);
//        CustomerResponse response = customerService.updateCustomerMulti2(customerId, request);
//        return ResponseEntity.ok(response);
//    }
    
   

}

