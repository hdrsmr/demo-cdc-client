
// untuk Member Mode

//package com.abc.hazelcast.cdc.config;
//
//import com.hazelcast.config.Config;
//import com.hazelcast.config.EvictionConfig;
//import com.hazelcast.config.EvictionPolicy;
//import com.hazelcast.config.MapConfig;
//import com.hazelcast.config.MaxSizePolicy;
//import com.hazelcast.config.NetworkConfig;
//import com.hazelcast.core.Hazelcast;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.spring.cache.HazelcastCacheManager;
//import org.springframework.cache.CacheManager;
//import org.springframework.cache.annotation.EnableCaching;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Primary;
//
//@Configuration
//@EnableCaching
//public class HazelcastConfig {
//
//    @Bean
//    @Primary
//    public HazelcastInstance hazelcastInstance() {
//        Config config = new Config();
//        config.setClusterName("hazelcast");
//        config.setInstanceName("hazelcast-member");
//
//        // Network configuration
//        NetworkConfig networkConfig = config.getNetworkConfig();
//        networkConfig.setPort(5701);
//        networkConfig.setPortAutoIncrement(true);
//        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
//        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
//        networkConfig.getJoin().getTcpIpConfig().addMember("127.0.0.1");
//        networkConfig.getJoin().getTcpIpConfig().addMember("192.168.181.96");
//
//        // ============================================
//        // MAP CUSTOMERS - LRU
//        // ============================================
//        MapConfig customerMapConfig = new MapConfig("customers");
//        customerMapConfig.setTimeToLiveSeconds(3600);
//        customerMapConfig.setMaxIdleSeconds(1800);
//        
//        EvictionConfig evictionConfig = new EvictionConfig()
//            .setEvictionPolicy(EvictionPolicy.LRU)
//            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
//            .setSize(1000);
//        customerMapConfig.setEvictionConfig(evictionConfig);
//        config.addMapConfig(customerMapConfig);
//
//        // ============================================
//        // MAP SESSIONS - LFU
//        // ============================================
//        MapConfig sessionMapConfig = new MapConfig("sessions");
//        sessionMapConfig.setTimeToLiveSeconds(1800);
//        sessionMapConfig.setMaxIdleSeconds(600);
//        
//        EvictionConfig sessionEviction = new EvictionConfig()
//            .setEvictionPolicy(EvictionPolicy.LFU)
//            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
//            .setSize(500);
//        sessionMapConfig.setEvictionConfig(sessionEviction);
//        config.addMapConfig(sessionMapConfig);
//
//        // ============================================
//        // MAP TEMP - TTL Only (No Eviction)
//        // ============================================
//        MapConfig tempMapConfig = new MapConfig("temp-cache");
//        tempMapConfig.setTimeToLiveSeconds(300);
//        tempMapConfig.setMaxIdleSeconds(60);
//        
//        EvictionConfig tempEviction = new EvictionConfig()
//            .setEvictionPolicy(EvictionPolicy.NONE)
//            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
//            .setSize(100);
//        tempMapConfig.setEvictionConfig(tempEviction);
//        config.addMapConfig(tempMapConfig);
//
//        return Hazelcast.newHazelcastInstance(config);
//    }
//
//    @Bean
//    public CacheManager cacheManager(HazelcastInstance hazelcastInstance) {
//        return new HazelcastCacheManager(hazelcastInstance);
//    }
//}


//package com.abc.hazelcast.cdc.config;



//old2 buat versi embedded

//import com.hazelcast.client.HazelcastClient;
//import com.hazelcast.client.config.ClientConfig;
//import com.hazelcast.client.config.ClientNetworkConfig;
//import com.hazelcast.config.Config;
//import com.hazelcast.config.EvictionConfig;
//import com.hazelcast.config.EvictionPolicy;
//import com.hazelcast.config.MapConfig;
//import com.hazelcast.config.MaxSizePolicy;
//import com.hazelcast.core.Hazelcast;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.spring.cache.HazelcastCacheManager;
//
//
//@Configuration
//@EnableCaching
//public class HazelcastConfig {
//
//    @Bean
//    @Primary
//    public HazelcastInstance hazelcastInstance() {
//        ClientConfig clientConfig = new ClientConfig();
//        
//        Config config = new Config();
//        MapConfig mapConfig = new MapConfig("customers");
//        mapConfig.setTimeToLiveSeconds(3600);
//        config.addMapConfig(mapConfig);
//        Hazelcast.newHazelcastInstance(config);
//        
//        clientConfig.setClusterName("hazelcast");
//        clientConfig.setInstanceName("hazelcast-client");
//        
//        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
//        networkConfig.addAddress("192.168.181.96:5701");
//        
//        // Connection settings yang tersedia di 5.7.0
//        networkConfig.setConnectionTimeout(5000);
//        
//        // Opsional: Set connection retry
//        clientConfig.setProperty("hazelcast.client.heartbeat.interval", "5000");
//        clientConfig.setProperty("hazelcast.client.heartbeat.timeout", "30000");
//        
//        // ============================================
//        // MAP CUSTOMERS - LRU
//        // ============================================
//        MapConfig customerMapConfig = new MapConfig("customers");
//        customerMapConfig.setTimeToLiveSeconds(3600);       // TTL 1 jam
//        customerMapConfig.setMaxIdleSeconds(1800);          // Idle 30 menit
//        
//        // Eviction LRU
//        EvictionConfig evictionConfig = new EvictionConfig()
//            .setEvictionPolicy(EvictionPolicy.LRU)
//            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
//            .setSize(1000);
//        customerMapConfig.setEvictionConfig(evictionConfig);
//        config.addMapConfig(customerMapConfig);
//
//        // ============================================
//        // MAP SESSIONS - LFU
//        // ============================================
//        MapConfig sessionMapConfig = new MapConfig("sessions");
//        sessionMapConfig.setTimeToLiveSeconds(1800);        // TTL 30 menit
//        sessionMapConfig.setMaxIdleSeconds(600);            // Idle 10 menit
//        
//        // Eviction LFU
//        EvictionConfig sessionEviction = new EvictionConfig()
//            .setEvictionPolicy(EvictionPolicy.LFU)
//            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
//            .setSize(500);
//        sessionMapConfig.setEvictionConfig(sessionEviction);
//        config.addMapConfig(sessionMapConfig);
//
//        // ============================================
//        // MAP TEMP - TTL Only (No Eviction)
//        // ============================================
//        MapConfig tempMapConfig = new MapConfig("temp-cache");
//        tempMapConfig.setTimeToLiveSeconds(300);            // TTL 5 menit
//        tempMapConfig.setMaxIdleSeconds(60);                // Idle 1 menit
//        
//        // No eviction
//        EvictionConfig tempEviction = new EvictionConfig()
//            .setEvictionPolicy(EvictionPolicy.NONE)
//            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
//            .setSize(100);
//        tempMapConfig.setEvictionConfig(tempEviction);
//        config.addMapConfig(tempMapConfig);
//
//        
//        return HazelcastClient.newHazelcastClient(clientConfig);
//    }
//
//    @Bean
//    public CacheManager cacheManager(HazelcastInstance hazelcastInstance) {
//        return new HazelcastCacheManager(hazelcastInstance);
//    }
//}

 