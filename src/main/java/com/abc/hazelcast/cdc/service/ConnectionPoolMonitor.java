package com.abc.hazelcast.cdc.service;



import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConnectionPoolMonitor {

    private final DataSource dataSource;

    public Map<String, Object> getPoolStats() {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            if (dataSource instanceof HikariDataSource) {
                HikariDataSource hikariDS = (HikariDataSource) dataSource;
                
                // Get HikariPool via reflection (because it's private)
                Field poolField = HikariDataSource.class.getDeclaredField("pool");
                poolField.setAccessible(true);
                HikariPool pool = (HikariPool) poolField.get(hikariDS);
                
                // Get pool statistics via reflection
                Field totalConnectionsField = HikariPool.class.getDeclaredField("totalConnections");
                totalConnectionsField.setAccessible(true);
                int totalConnections = totalConnectionsField.getInt(pool);
                
                Field idleConnectionsField = HikariPool.class.getDeclaredField("idleConnections");
                idleConnectionsField.setAccessible(true);
                int idleConnections = idleConnectionsField.getInt(pool);
                
                Field activeConnectionsField = HikariPool.class.getDeclaredField("activeConnections");
                activeConnectionsField.setAccessible(true);
                int activeConnections = activeConnectionsField.getInt(pool);
                
                stats.put("totalConnections", totalConnections);
                stats.put("idleConnections", idleConnections);
                stats.put("activeConnections", activeConnections);
                stats.put("maximumPoolSize", hikariDS.getMaximumPoolSize());
                stats.put("minimumIdle", hikariDS.getMinimumIdle());
                stats.put("connectionTimeout", hikariDS.getConnectionTimeout());
                stats.put("poolName", hikariDS.getPoolName());
                
                log.info("HikariCP Stats: Total={}, Idle={}, Active={}, Max={}", 
                    totalConnections, idleConnections, activeConnections, hikariDS.getMaximumPoolSize());
            }
        } catch (Exception e) {
            log.error("Failed to get pool stats: {}", e.getMessage());
            stats.put("error", e.getMessage());
        }
        
        return stats;
    }
}