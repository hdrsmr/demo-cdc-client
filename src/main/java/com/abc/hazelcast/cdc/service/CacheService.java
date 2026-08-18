package com.abc.hazelcast.cdc.service;


import java.util.Map;

import com.abc.hazelcast.cdc.dto.CustomerResponse;


public interface CacheService {

    void evictCustomer(Integer customerId);

    void clearAllCache();

    void evictCache(String key);

    void addTempCache(String key, String value);

    String getTempCache(String key);

    void addTempCacheMap(String key, Map<String, Object> value);

    Map<String, Object> getTempCacheMap(String key);

    void addCustomerWithTTL(Integer customerId, CustomerResponse customer);

    CustomerResponse getCustomerFromCache(Integer customerId);

    Map<String, Object> getCacheStats();


}

