package com.loopers.application.ranking;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ranking.fallback-snapshot")
public record RankingFallbackSnapshotProperties(
    int topN
) {
    public RankingFallbackSnapshotProperties {
        if (topN < 1) {
            topN = 100;
        }
    }
}
