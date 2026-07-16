package com.loopers.infrastructure.ranking;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ranking.carry-over")
public record RankingCarryOverProperties(double weight) {

    public RankingCarryOverProperties {
        if (weight <= 0 || weight > 1) {
            weight = 0.05;
        }
    }
}