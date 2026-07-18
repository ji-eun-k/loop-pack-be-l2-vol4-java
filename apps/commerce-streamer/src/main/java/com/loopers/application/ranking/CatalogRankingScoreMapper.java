package com.loopers.application.ranking;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.ranking.RankingScorePolicy;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * 카탈로그 이벤트 payload를 파싱해 상품별 랭킹 점수 증감(delta)으로 변환한다.
 * 실시간 랭킹 반영(CatalogRankingProjectorConsumer)과 원장 리플레이(RankingRecoveryService)가
 * 동일한 매핑 로직을 공유하도록 이 클래스에 계산을 단일화했다.
 */
@Component
@RequiredArgsConstructor
public class CatalogRankingScoreMapper {

    private final ObjectMapper objectMapper;
    private final RankingScorePolicy scorePolicy;

    public Map<Long, Double> map(String eventType, String payload) throws Exception {
        Map<String, Object> data = objectMapper.readValue(payload, new TypeReference<>() {});
        Map<Long, Double> deltas = new HashMap<>();
        switch (eventType) {
            case "ProductViewedEvent" -> deltas.put(productId(data), scorePolicy.viewScore());
            case "ProductLikedEvent" -> deltas.put(productId(data), scorePolicy.likeScore());
            case "ProductUnlikedEvent" -> deltas.put(productId(data), scorePolicy.unlikeScore());
            case "OrderItemSoldEvent" -> {
                // 구버전 이벤트 등 금액 정보가 없는 경우 점수 반영 없이 넘어간다.
                if (data.get("productAmountMap") == null) {
                    return deltas;
                }
                Map<String, BigDecimal> amounts = objectMapper.convertValue(
                    data.get("productAmountMap"), new TypeReference<>() {}
                );
                amounts.forEach((id, amount) -> deltas.merge(
                    Long.parseLong(id), scorePolicy.orderScore(amount.longValue()), Double::sum
                ));
            }
            default -> { }
        }
        return deltas;
    }

    private long productId(Map<String, Object> data) {
        return ((Number) data.get("productId")).longValue();
    }
}
