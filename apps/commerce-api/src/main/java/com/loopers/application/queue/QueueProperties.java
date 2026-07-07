package com.loopers.application.queue;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "queue")
@Getter
@Setter
public class QueueProperties {
    private int batchSize = 75;
    private long schedulerIntervalMs = 1000;
    private long tokenTtlSeconds = 300;
    private long maxWaitingSize = 100000;
}
