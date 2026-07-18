package com.loopers.domain.ranking;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** ranking.weight.* 설정값. view/like/order 행동별 랭킹 점수 가중치. */
@ConfigurationProperties(prefix = "ranking.weight")
public record RankingWeightProperties(double view, double like, double order) {
}
