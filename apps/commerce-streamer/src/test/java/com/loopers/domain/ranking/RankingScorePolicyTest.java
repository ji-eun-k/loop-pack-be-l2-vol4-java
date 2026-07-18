package com.loopers.domain.ranking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class RankingScorePolicyTest {

    private RankingScorePolicy policy;

    @BeforeEach
    void setUp() {
        policy = new RankingScorePolicy(new RankingWeightProperties(0.1, 0.2, 0.6));
    }

    @DisplayName("점수를 계산할 때,")
    @Nested
    class Score {

        @DisplayName("조회는 view 가중치만큼의 점수를 반환한다.")
        @Test
        void returnsViewWeight_whenViewed() {
            double score = policy.viewScore();

            assertThat(score).isEqualTo(0.1);
        }

        @DisplayName("좋아요는 like 가중치만큼의 점수를 반환한다.")
        @Test
        void returnsLikeWeight_whenLiked() {
            double score = policy.likeScore();

            assertThat(score).isEqualTo(0.2);
        }

        @DisplayName("좋아요 취소는 like 가중치의 음수 점수를 반환한다.")
        @Test
        void returnsNegativeLikeWeight_whenUnliked() {
            double score = policy.unlikeScore();

            assertThat(score).isEqualTo(-0.2);
        }

        @DisplayName("주문은 order 가중치 × log10(1 + 주문금액)으로 정규화된 점수를 반환한다.")
        @Test
        void returnsLogNormalizedScore_whenOrdered() {
            double score = policy.orderScore(100_000L);

            assertThat(score).isCloseTo(0.6 * Math.log10(1 + 100_000L), within(1e-9));
        }

        @DisplayName("주문금액이 0이면 주문 점수는 0이다.")
        @Test
        void returnsZero_whenOrderAmountIsZero() {
            double score = policy.orderScore(0L);

            assertThat(score).isEqualTo(0.0);
        }

        @DisplayName("최저가(1,000원) 주문 1건의 점수가 좋아요 3건의 점수보다 크다.")
        @Test
        void orderScoreBeatsThreeLikes_evenAtMinimumPrice() {
            double orderScore = policy.orderScore(1_000L);
            double threeLikes = policy.likeScore() * 3;

            assertThat(orderScore).isGreaterThan(threeLikes);
        }

        @DisplayName("주문금액 1,000배 차이가 점수를 압도하지 않는다 — 저가 상품 주문 2건이 고가 상품 주문 1건을 이긴다.")
        @Test
        void twoCheapOrdersBeatOneExpensiveOrder() {
            double cheapTwice = policy.orderScore(1_000L) * 2;
            double expensiveOnce = policy.orderScore(1_000_000L);

            assertThat(cheapTwice).isGreaterThan(expensiveOnce);
        }
    }
}
