package com.loopers.domain.ranking;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 유저 행동별 랭킹 점수 계산 정책.
 * 주문은 log10 정규화로 금액 격차를 압축해 주문 건수 중심의 랭킹이 되도록 한다.
 */
@RequiredArgsConstructor
@Component
public class RankingScorePolicy {

    private final RankingWeightProperties weights;

    public double viewScore() {
        return weights.view();
    }

    public double likeScore() {
        return weights.like();
    }

    public double unlikeScore() {
        return -weights.like();
    }

    public double orderScore(long amount) {
        return weights.order() * Math.log10(1 + amount);
    }
}
