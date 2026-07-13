package com.loopers.domain.ranking;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ranking.weight")
public record RankingWeightProperties(double view, double like, double order) {
}
