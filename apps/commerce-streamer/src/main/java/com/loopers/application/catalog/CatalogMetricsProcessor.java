package com.loopers.application.catalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.ranking.RankingScorePolicy;
import com.loopers.infrastructure.catalog.EventHandledEntity;
import com.loopers.infrastructure.catalog.EventHandledJpaRepository;
import com.loopers.infrastructure.catalog.ProductMetricsJpaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Component
public class CatalogMetricsProcessor {

    private final EventHandledJpaRepository eventHandledJpaRepository;
    private final ProductMetricsJpaRepository productMetricsJpaRepository;
    private final ObjectMapper objectMapper;
    private final RankingScorePolicy rankingScorePolicy;

    /**
     * 이벤트를 처리하고 랭킹 ZSET에 반영할 상품별 점수 델타를 반환한다.
     * 중복 eventId의 DB metrics 갱신은 건너뛰되, Redis 장애 재처리를 위해 랭킹 델타는 다시 반환한다.
     * 랭킹 이중 적립은 Redis Lua script의 eventId 집합으로 방지한다.
     */
    @Transactional
    public Map<Long, Double> process(String eventType, String eventId, String topic, String payload) throws Exception {
        Map<String, Object> data = objectMapper.readValue(payload, new TypeReference<>() {});
        Map<Long, Double> rankingDeltas = calculateRankingDeltas(eventType, eventId, data);

        if (eventHandledJpaRepository.existsByEventId(eventId)) {
            log.debug("[CATALOG] 중복 이벤트 skip — eventId={}, eventType={}", eventId, eventType);
            // DB metrics는 멱등 처리하지만 Redis 반영 실패 후의 재소비를 위해 점수 델타는 다시 반환한다.
            // Redis에서는 eventId 기반 Lua script가 이중 적립을 막는다.
            return rankingDeltas;
        }

        switch (eventType) {
            case "OrderItemSoldEvent" -> {
                Map<String, Integer> productQtyMap = objectMapper.convertValue(
                    data.get("productQtyMap"), new TypeReference<>() {}
                );
                productQtyMap.forEach((productId, qty) ->
                    productMetricsJpaRepository.upsertOrderCount(Long.parseLong(productId), qty)
                );
            }
            case "ProductLikedEvent" -> {
                Long productId = ((Number) data.get("productId")).longValue();
                productMetricsJpaRepository.upsertLikeCountIncrement(productId);
            }
            case "ProductUnlikedEvent" -> {
                Long productId = ((Number) data.get("productId")).longValue();
                productMetricsJpaRepository.upsertLikeCountDecrement(productId);
            }
            default -> log.warn("[CATALOG] 알 수 없는 이벤트 타입 — eventType={}", eventType);
        }

        eventHandledJpaRepository.save(new EventHandledEntity(eventId, topic));
        return rankingDeltas;
    }

    private Map<Long, Double> calculateRankingDeltas(
        String eventType,
        String eventId,
        Map<String, Object> data
    ) {
        Map<Long, Double> deltas = new HashMap<>();
        switch (eventType) {
            case "OrderItemSoldEvent" -> {
                if (data.get("productAmountMap") == null) {
                    log.debug("[CATALOG] productAmountMap 없음(구버전 이벤트) — 랭킹 점수 미적립, eventId={}", eventId);
                    return deltas;
                }
                Map<String, BigDecimal> productAmountMap = objectMapper.convertValue(
                    data.get("productAmountMap"), new TypeReference<>() {}
                );
                productAmountMap.forEach((productId, amount) -> deltas.merge(
                    Long.parseLong(productId), rankingScorePolicy.orderScore(amount.longValue()), Double::sum
                ));
            }
            case "ProductLikedEvent" -> {
                Long productId = ((Number) data.get("productId")).longValue();
                deltas.put(productId, rankingScorePolicy.likeScore());
            }
            case "ProductUnlikedEvent" -> {
                Long productId = ((Number) data.get("productId")).longValue();
                deltas.put(productId, rankingScorePolicy.unlikeScore());
            }
            default -> {
                // metrics 처리부에서 알 수 없는 타입을 기록한다.
            }
        }
        return deltas;
    }
}
