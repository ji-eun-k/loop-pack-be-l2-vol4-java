package com.loopers.application.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.order.OrderService;
import com.loopers.domain.order.OrderItem;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;

@ExtendWith(MockitoExtension.class)
class UserActionEventListenerTest {

    @Mock
    private OrderService orderService;

    @Mock
    private OutboxService outboxService;

    @Mock
    private KafkaTemplate<Object, Object> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private UserActionEventListener listener;

    @BeforeEach
    void setUp() {
        listener = new UserActionEventListener(
            orderService, outboxService, kafkaTemplate, objectMapper,
            Clock.fixed(Instant.parse("2026-07-12T14:59:59Z"), ZoneId.of("Asia/Seoul"))
        );
    }

    @DisplayName("주문 완료 이벤트를 처리할 때,")
    @Nested
    class OrderCompleted {

        @DisplayName("페이로드에 productQtyMap과 productAmountMap(가격×수량)이 포함된다.")
        @Test
        void includesProductAmountMap_inOrderItemSoldEventPayload() throws Exception {
            Long orderId = 1L;
            given(orderService.getOrderItems(orderId)).willReturn(List.of(
                new OrderItem(10L, "상품A", BigDecimal.valueOf(12_000), 2),
                new OrderItem(20L, "상품B", BigDecimal.valueOf(5_000), 3)
            ));

            listener.handle(new UserActionEvent(UserActionType.ORDER_COMPLETED, 99L, orderId));

            ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
            then(outboxService).should().save(captor.capture());
            JsonNode payload = objectMapper.readTree(captor.getValue().getPayload());

            assertThat(captor.getValue().getEventType()).isEqualTo("OrderItemSoldEvent");
            assertThat(payload.path("productQtyMap").path("10").asInt()).isEqualTo(2);
            assertThat(payload.path("productQtyMap").path("20").asInt()).isEqualTo(3);
            assertThat(payload.path("productAmountMap").path("10").asLong()).isEqualTo(24_000L);
            assertThat(payload.path("productAmountMap").path("20").asLong()).isEqualTo(15_000L);
        }
    }

    @DisplayName("상품 조회 이벤트를 처리할 때,")
    @Nested
    class ProductViewed {

        @DisplayName("재처리 멱등성과 발생일 계산에 필요한 eventId와 occurredAt을 발행한다.")
        @Test
        void includesEventIdentityAndOccurredAt() throws Exception {
            listener.handleProductViewed(new UserActionEvent(UserActionType.PRODUCT_VIEWED, null, 10L));

            ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
            then(kafkaTemplate).should().send(eq("catalog-view-events-v1"), isNull(), payloadCaptor.capture());
            JsonNode payload = objectMapper.readTree(payloadCaptor.getValue());

            assertThat(payload.path("productId").asLong()).isEqualTo(10L);
            assertThat(payload.path("eventId").asText()).isNotBlank();
            assertThat(payload.path("occurredAt").asText()).startsWith("2026-07-12T23:59:59+09:00");
        }
    }
}
