package com.abc.hazelcast.cdc.service;

import com.abc.hazelcast.cdc.dto.CustomerRequest;
import com.abc.hazelcast.cdc.dto.CustomerResponse;
import com.abc.hazelcast.cdc.dto.PageResponse;
import java.util.List;
import java.util.Map;


public interface CustomerService {

    CustomerResponse createCustomer(CustomerRequest request);

    CustomerResponse getCustomerById(Integer id);

	List<CustomerResponse> getAllFromCache();

	void evictCache(String cacheKey);
	
	CustomerResponse updateCustomerMulti2(Integer customerId, Map<String, Object> updates);

	PageResponse<CustomerResponse> getAllCustomers(int page, int size, String sortBy, String sortDirection);

	void deleteCustomer(Integer id);


}