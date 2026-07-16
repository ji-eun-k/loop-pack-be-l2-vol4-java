package com.loopers.application.ranking;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.ranking.RankingScorePolicy;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

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
