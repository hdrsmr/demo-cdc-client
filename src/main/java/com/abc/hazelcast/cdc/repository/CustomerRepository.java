package com.abc.hazelcast.cdc.repository;

import com.abc.hazelcast.cdc.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CustomerRepository extends JpaRepository<Customer, Integer> {

    // Check if customer code exists
    boolean existsByCustomerCode(String customerCode);


}