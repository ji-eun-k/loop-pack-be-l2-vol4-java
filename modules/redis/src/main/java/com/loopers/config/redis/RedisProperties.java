package com.loopers.config.redis;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties(value = "datasource.redis")
public record RedisProperties(
        int database,
        RedisNodeInfo master,
        List<RedisNodeInfo> replicas,
        Duration commandTimeout
) {
    private static final Duration DEFAULT_COMMAND_TIMEOUT = Duration.ofSeconds(3);

    public RedisProperties {
        if (commandTimeout == null) {
            commandTimeout = DEFAULT_COMMAND_TIMEOUT;
        }
    }
}