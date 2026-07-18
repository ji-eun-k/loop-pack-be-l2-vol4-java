package com.loopers.infrastructure.ranking;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** ranking.carry-over.weight: 자정 전환 시 다음날 키에 미리 반영할 오늘 점수의 비율(기본 5%). */
@ConfigurationProperties(prefix = "ranking.carry-over")
public record RankingCarryOverProperties(double weight) {

    public RankingCarryOverProperties {
        if (weight <= 0 || weight > 1) {
            weight = 0.05;
        }
    }
}