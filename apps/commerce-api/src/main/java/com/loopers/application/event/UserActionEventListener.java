package com.loopers.application.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.order.OrderService;
import com.loopers.domain.order.OrderItem;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Component
public class UserActionEventListener {

    private static final String TOPIC = "catalog-events-v1";
    private static final String VIEW_TOPIC = "catalog-view-events-v1";

    private final OrderService orderService;
    private final OutboxService outboxService;
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handle(UserActionEvent event) {
        try {
            OutboxEvent outboxEvent = buildOutboxEvent(event);
            if (outboxEvent != null) {
                outboxService.save(outboxEvent);
            }
        } catch (Exception e) {
            log.error("[USER_ACTION] outbox 저장 실패 — action={}, userId={}, resourceId={}",
                event.actionType(), event.userId(), event.resourceId(), e);
        }
    }

    @Async
    @EventListener
    public void handleProductViewed(UserActionEvent event) {
        if (event.actionType() != UserActionType.PRODUCT_VIEWED) {
            return;
        }
        try {
            String payload = objectMapper.writeValueAsString(Map.of("productId", event.resourceId()));
            // partition key = null (round-robin) — view_count는 순서 무관한 누적 연산이므로 균등 분산 우선
            // topic 분리 이유:
            //   - 볼륨: 조회 이벤트는 주문/좋아요 대비 압도적으로 많아 같은 토픽에 넣으면 consumer 처리 병목
            //   - retention: view 이벤트가 retention.bytes 한도에 닿으면 order/like 이벤트까지 삭제될 수 있음
            kafkaTemplate.send(VIEW_TOPIC, null, payload);
        } catch (JsonProcessingException e) {
            log.warn("[PRODUCT_VIEWED] Kafka 발행 실패 — productId={}", event.resourceId(), e);
        }
    }

    private OutboxEvent buildOutboxEvent(UserActionEvent event) throws JsonProcessingException {
        return switch (event.actionType()) {
            case ORDER_COMPLETED -> buildOrderCompletedEvent(event);
            case PRODUCT_LIKE -> buildProductLikedEvent(event);
            case PRODUCT_UNLIKE -> buildProductUnlikedEvent(event);
            case PRODUCT_VIEWED -> null;
        };
    }

    private OutboxEvent buildOrderCompletedEvent(UserActionEvent event) throws JsonProcessingException {
        Long orderId = event.resourceId();
        List<OrderItem> items = orderService.getOrderItems(orderId);
        Map<Long, Integer> productQtyMap = items.stream()
            .collect(Collectors.toMap(OrderItem::getProductId, OrderItem::getQuantity));

        String payload = objectMapper.writeValueAsString(Map.of(
            "orderId", orderId,
            "productQtyMap", productQtyMap
        ));

        return new OutboxEvent("OrderItemSoldEvent", payload, TOPIC, orderId.toString());
    }

    private OutboxEvent buildProductLikedEvent(UserActionEvent event) throws JsonProcessingException {
        String payload = objectMapper.writeValueAsString(Map.of("productId", event.resourceId()));
        return new OutboxEvent("ProductLikedEvent", payload, TOPIC, event.userId().toString());
    }

    private OutboxEvent buildProductUnlikedEvent(UserActionEvent event) throws JsonProcessingException {
        String payload = objectMapper.writeValueAsString(Map.of("productId", event.resourceId()));
        return new OutboxEvent("ProductUnlikedEvent", payload, TOPIC, event.userId().toString());
    }
}
