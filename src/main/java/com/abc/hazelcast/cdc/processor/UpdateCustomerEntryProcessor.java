package com.abc.hazelcast.cdc.processor;


import com.abc.hazelcast.cdc.dto.CustomerResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.EntryProcessor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

//  EntryProcessor harus Serializable
public class UpdateCustomerEntryProcessor implements EntryProcessor<Integer, HazelcastJsonValue, CustomerResponse>, Serializable {

    private static final long serialVersionUID = 1L;
    
    private final Map<String, Object> updates;
    private transient ObjectMapper objectMapper;

    //  Constructor menerima ObjectMapper
    public UpdateCustomerEntryProcessor(Map<String, Object> updates, ObjectMapper objectMapper) {
        this.updates = updates != null ? new HashMap<>(updates) : new HashMap<>();
        this.objectMapper = objectMapper;
    }

    @Override
    public CustomerResponse process(Map.Entry<Integer, HazelcastJsonValue> entry) {
        if (entry.getValue() == null) {
            return null;
        }

        try {
            String json = entry.getValue().toString();
            Map<String, Object> customerMap = objectMapper.readValue(json, Map.class);
            
            //  Update multiple fields
            updates.forEach((key, value) -> {
                if ("customerId".equals(key)) {
                    throw new IllegalArgumentException("customerId cannot be updated");
                }
                customerMap.put(key, value);
            });
            
            customerMap.put("updatedDate", LocalDateTime.now().toString());
            customerMap.put("source", "HAZELCAST_ENTRY_PROCESSOR_UPDATED");
            
            String updatedJson = objectMapper.writeValueAsString(customerMap);
            HazelcastJsonValue updatedValue = new HazelcastJsonValue(updatedJson);
            entry.setValue(updatedValue);
            
            return parseCustomerResponse(customerMap);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to update customer: " + e.getMessage(), e);
        }
    }

    private CustomerResponse parseCustomerResponse(Map<String, Object> map) {
        CustomerResponse response = new CustomerResponse();
        response.setCustomerId((Integer) map.get("customerId"));
        response.setCustomerCode((String) map.get("customerCode"));
        response.setCustomerName((String) map.get("customerName"));
        response.setEmail((String) map.get("email"));
        response.setCity((String) map.get("city"));
//        response.setCreatedDate((String) map.get("createdDate"));
//        response.setUpdatedDate((String) map.get("updatedDate"));
//        response.setCreatedDate((LocalDateTime) map.get("createdDate"));
//        response.setUpdatedDate((LocalDateTime) map.get("updatedDate"));
        response.setSource((String) map.get("source"));
        return response;
    }

  //  @Override
    public CustomerResponse getDefaultValue() {
        return null;
    }
}