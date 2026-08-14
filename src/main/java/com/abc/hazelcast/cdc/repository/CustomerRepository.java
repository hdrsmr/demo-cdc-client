package com.abc.hazelcast.cdc.repository;



//@Repository
//public interface CustomerRepository extends JpaRepository<Customer, Integer> {
//
//    Optional<Customer> findByCustomerCode(String customerCode);
//
//    boolean existsByCustomerCode(String customerCode);
//}



import com.abc.hazelcast.cdc.entity.Customer;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.query.Procedure;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public interface CustomerRepository extends JpaRepository<Customer, Integer> {

    // Find by customer code (unique)
    Optional<Customer> findByCustomerCode(String customerCode);

    // Check if customer code exists
    boolean existsByCustomerCode(String customerCode);

    // Check if customer code exists excluding a specific ID (for update)
    @Query("SELECT COUNT(c) > 0 FROM Customer c WHERE c.customerCode = :customerCode AND c.customerId != :customerId")
    boolean existsByCustomerCodeAndIdNot(@Param("customerCode") String customerCode, 
                                         @Param("customerId") Integer customerId);

    // Search by customer name (contains)
    Page<Customer> findByCustomerNameContainingIgnoreCase(String customerName, Pageable pageable);

    // Search by city
    Page<Customer> findByCityIgnoreCase(String city, Pageable pageable);

    // Search by customer name or city
    @Query("SELECT c FROM Customer c WHERE " +
            "LOWER(c.customerCode) LIKE LOWER(CONCAT('%', :keyword, '%')) OR " +
            "LOWER(c.customerName) LIKE LOWER(CONCAT('%', :keyword, '%')) OR " +
            "LOWER(c.city) LIKE LOWER(CONCAT('%', :keyword, '%')) OR " +
            "LOWER(c.email) LIKE LOWER(CONCAT('%', :keyword, '%'))")
     Page<Customer> searchCustomers(@Param("keyword") String keyword, Pageable pageable);
    
    
    //sp
    @Modifying
    @Query(value = "EXEC sp_UpdateCustomerEmail :customerCode, :newEmail", 
           nativeQuery = true)
    int updateCustomerEmail(@Param("customerCode") String customerCode, 
                            @Param("newEmail") String newEmail);
    
   
    // Untuk stored procedure dengan output parameters
    @Procedure(name = "sp_UpdateCustomerEmailWithOutput")
    Map<String, Object> updateCustomerEmailWithOutput(
        @Param("customer_code") String customerCode,
        @Param("new_email") String newEmail
    );
    
    // Atau jika stored procedure mengembalikan result set
    @Procedure(name = "sp_UpdateCustomerEmailWithOutput")
    List<Object[]> updateCustomerEmailWithOutputReturnResultSet(
        @Param("customer_code") String customerCode,
        @Param("new_email") String newEmail
    );

    @Modifying
    @Query("UPDATE Customer c SET c.email = :newEmail, c.updatedDate = CURRENT_TIMESTAMP WHERE c.customerCode = :customerCode")
    int updateEmailByCustomerCode(@Param("customerCode") String customerCode, 
                                   @Param("newEmail") String newEmail);
    
}