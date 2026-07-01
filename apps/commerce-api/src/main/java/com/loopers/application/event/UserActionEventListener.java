package com.loopers.application.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.order.OrderService;
import com.loopers.domain.order.OrderItem;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Component
public class UserActionEventListener {

    private static final String TOPIC = "catalog-events-v1";

    private final OrderService orderService;
    private final OutboxService outboxService;
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

    private OutboxEvent buildOutboxEvent(UserActionEvent event) throws JsonProcessingException {
        return switch (event.actionType()) {
            case ORDER_COMPLETED -> buildOrderCompletedEvent(event);
            case PRODUCT_LIKE -> buildProductLikedEvent(event);
            case PRODUCT_UNLIKE -> buildProductUnlikedEvent(event);
            // TODO: PRODUCT_VIEWED는 outbox 대신 Kafka 직접 발행으로 처리
            // - topic: catalog-view-events-v1 (catalog-events-v1 과 분리)
            //   이유 1 (볼륨): 조회 이벤트는 주문/좋아요 대비 압도적으로 볼륨이 높아,
            //     같은 토픽에 넣으면 consumer가 대량의 view 메시지 속에서 order/like 메시지를 처리해야 함.
            //     Confluent/Martin Kleppmann 권장: 고볼륨·저볼륨 스트림은 토픽 분리.
            //   이유 2 (retention): Kafka 토픽은 retention.bytes(용량 한도)를 공유함.
            //     retention = "Kafka가 메시지를 얼마나 오래/얼마나 많이 보관할지" 설정.
            //     view 이벤트가 한도에 닿으면 오래된 메시지부터 삭제되는데,
            //     같은 토픽이면 order/like 이벤트까지 같이 밀려서 삭제될 수 있음.
            //     토픽을 분리하면 "조회는 1일, 주문은 30일" 처럼 독립적으로 설정 가능.
            //     (Netflix, Uber 모두 이 이유로 topic-per-event-type 구조 채택)
            // - partition key: null (round-robin) — view_count는 순서 무관한 누적 연산이므로 균등 분산 우선
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

        return new OutboxEvent("OrderItemSoldEvent", payload, TOPIC, orderId.toString(), orderId.toString());
    }

    private OutboxEvent buildProductLikedEvent(UserActionEvent event) throws JsonProcessingException {
        String payload = objectMapper.writeValueAsString(Map.of("productId", event.resourceId()));
        return new OutboxEvent("ProductLikedEvent", payload, TOPIC, event.userId().toString(), UUID.randomUUID().toString());
    }

    private OutboxEvent buildProductUnlikedEvent(UserActionEvent event) throws JsonProcessingException {
        String payload = objectMapper.writeValueAsString(Map.of("productId", event.resourceId()));
        return new OutboxEvent("ProductUnlikedEvent", payload, TOPIC, event.userId().toString(), UUID.randomUUID().toString());
    }
}
