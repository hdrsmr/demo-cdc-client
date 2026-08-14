package com.abc.hazelcast.cdc.config;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@EnableCaching
public class HazelcastClientConfig {

    @Bean
    @Primary
    public HazelcastInstance hazelcastInstance() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("hazelcast");
        clientConfig.setInstanceName("hazelcast-client");

        // Network configuration
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress("192.168.181.96:5701");
        networkConfig.setConnectionTimeout(5000);

        // Client properties
        clientConfig.setProperty("hazelcast.client.heartbeat.interval", "5000");
        clientConfig.setProperty("hazelcast.client.heartbeat.timeout", "30000");

        // ============================================
        // NEAR CACHE - Cache di sisi client
        //  Hapus cache-local-entries (tidak didukung di client)
        // ============================================
        NearCacheConfig nearCacheConfig = new NearCacheConfig("customers");
        nearCacheConfig.setTimeToLiveSeconds(3600);
        nearCacheConfig.setMaxIdleSeconds(1800);
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        //  HAPUS: nearCacheConfig.setCacheLocalEntries(true);
        
        clientConfig.addNearCacheConfig(nearCacheConfig);

        // ============================================
        // NEAR CACHE untuk SESSIONS
        // ============================================
        NearCacheConfig sessionNearCache = new NearCacheConfig("sessions");
        sessionNearCache.setTimeToLiveSeconds(1800);
        sessionNearCache.setMaxIdleSeconds(600);
        sessionNearCache.setInMemoryFormat(InMemoryFormat.BINARY);
        clientConfig.addNearCacheConfig(sessionNearCache);

        // ============================================
        // NEAR CACHE untuk TEMP-CACHE
        // ============================================
        NearCacheConfig tempNearCache = new NearCacheConfig("temp-cache");
        tempNearCache.setTimeToLiveSeconds(300);
        tempNearCache.setMaxIdleSeconds(60);
        tempNearCache.setInMemoryFormat(InMemoryFormat.BINARY);
        clientConfig.addNearCacheConfig(tempNearCache);

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Bean
    public CacheManager cacheManager(HazelcastInstance hazelcastInstance) {
        return new HazelcastCacheManager(hazelcastInstance);
    }
}


