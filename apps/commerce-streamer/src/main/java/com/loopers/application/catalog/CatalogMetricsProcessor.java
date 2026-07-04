package com.loopers.application.catalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.infrastructure.catalog.EventHandledEntity;
import com.loopers.infrastructure.catalog.EventHandledJpaRepository;
import com.loopers.infrastructure.catalog.ProductMetricsJpaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Component
public class CatalogMetricsProcessor {

    private final EventHandledJpaRepository eventHandledJpaRepository;
    private final ProductMetricsJpaRepository productMetricsJpaRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void process(String eventType, String eventId, String topic, String payload) throws Exception {
        if (eventHandledJpaRepository.existsByEventId(eventId)) {
            log.debug("[CATALOG] 중복 이벤트 skip — eventId={}, eventType={}", eventId, eventType);
            return;
        }

        Map<String, Object> data = objectMapper.readValue(payload, new TypeReference<>() {});

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
    }
}
