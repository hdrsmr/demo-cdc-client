package com.abc.hazelcast.cdc.dto;


import java.time.LocalDateTime;

import com.abc.hazelcast.cdc.entity.Customer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class CustomerResponse {
    private Integer customerId;
    private String customerCode;
    private String customerName;
    private String email;
    private String city;
    private LocalDateTime createdDate;  //  Ganti dari LocalDateTime ke String
    private LocalDateTime updatedDate;  //  Ganti dari LocalDateTime ke String
    private String source;
    
    public static CustomerResponse fromEntity(Customer customer, String source) {
        CustomerResponse response = new CustomerResponse();
        response.setCustomerId(customer.getCustomerId());
        response.setCustomerCode(customer.getCustomerCode());
        response.setCustomerName(customer.getCustomerName());
        response.setEmail(customer.getEmail());
        response.setCity(customer.getCity());
        response.setCreatedDate(customer.getCreatedDate());
        response.setUpdatedDate(customer.getUpdatedDate());
        response.setSource(source);
        return response;
    }
}

