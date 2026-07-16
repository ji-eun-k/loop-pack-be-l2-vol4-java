package com.loopers.application.ranking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.ranking.RankingScorePolicy;
import com.loopers.domain.ranking.RankingWeightProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class CatalogRankingScoreMapperTest {

    private final CatalogRankingScoreMapper mapper = new CatalogRankingScoreMapper(
        new ObjectMapper(), new RankingScorePolicy(new RankingWeightProperties(0.1, 0.2, 0.6))
    );

    @DisplayName("OrderItemSoldEvent는 productAmountMap 기준으로 상품별 주문 점수 델타를 반환한다.")
    @Test
    void returnsOrderDeltas_whenOrderItemSoldEventHasAmountMap() throws Exception {
        Map<Long, Double> deltas = mapper.map("OrderItemSoldEvent",
            "{\"orderId\":1,\"productQtyMap\":{\"10\":2,\"20\":3},\"productAmountMap\":{\"10\":24000,\"20\":15000}}");

        assertThat(deltas.get(10L)).isCloseTo(0.6 * Math.log10(1 + 24_000), within(1e-9));
        assertThat(deltas.get(20L)).isCloseTo(0.6 * Math.log10(1 + 15_000), within(1e-9));
    }

    @DisplayName("OrderItemSoldEvent에 productAmountMap이 없으면(구버전 이벤트) 빈 델타를 반환한다.")
    @Test
    void returnsEmptyDeltas_whenAmountMapMissing() throws Exception {
        Map<Long, Double> deltas = mapper.map("OrderItemSoldEvent", "{\"orderId\":1,\"productQtyMap\":{\"10\":2}}");

        assertThat(deltas).isEmpty();
    }

    @DisplayName("ProductViewedEvent는 view 가중치 델타를 반환한다.")
    @Test
    void returnsViewDelta_whenProductViewedEvent() throws Exception {
        Map<Long, Double> deltas = mapper.map("ProductViewedEvent", "{\"productId\":10}");

        assertThat(deltas).containsEntry(10L, 0.1);
    }

    @DisplayName("ProductLikedEvent는 like 가중치 델타를 반환한다.")
    @Test
    void returnsLikeDelta_whenProductLikedEvent() throws Exception {
        Map<Long, Double> deltas = mapper.map("ProductLikedEvent", "{\"productId\":10}");

        assertThat(deltas).containsEntry(10L, 0.2);
    }

    @DisplayName("ProductUnlikedEvent는 like 가중치의 음수 델타를 반환한다.")
    @Test
    void returnsNegativeLikeDelta_whenProductUnlikedEvent() throws Exception {
        Map<Long, Double> deltas = mapper.map("ProductUnlikedEvent", "{\"productId\":10}");

        assertThat(deltas).containsEntry(10L, -0.2);
    }

    @DisplayName("알 수 없는 이벤트 타입이면 빈 델타를 반환한다.")
    @Test
    void returnsEmptyDeltas_whenUnknownEventType() throws Exception {
        Map<Long, Double> deltas = mapper.map("UnknownEvent", "{}");

        assertThat(deltas).isEmpty();
    }
}