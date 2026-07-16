package com.loopers.application.ranking;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "ranking.recovery")
public record RankingRecoveryProperties(
    int batchSize,
    Duration lockTtl,
    Duration markerTtl
) {
    public RankingRecoveryProperties {
        if (batchSize <= 0) {
            batchSize = 1_000;
        }
        if (lockTtl == null || lockTtl.isNegative() || lockTtl.isZero()) {
            lockTtl = Duration.ofMinutes(30);
        }
        if (markerTtl == null || markerTtl.isNegative() || markerTtl.isZero()) {
            markerTtl = Duration.ofDays(2);
        }
    }
}