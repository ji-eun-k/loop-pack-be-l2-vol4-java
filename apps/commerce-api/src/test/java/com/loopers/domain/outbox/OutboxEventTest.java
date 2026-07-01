package com.loopers.domain.outbox;

import com.loopers.support.error.CoreException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OutboxEventTest {

    @DisplayName("OutboxEvent를 생성할 때,")
    @Nested
    class Create {

        @DisplayName("유효한 정보를 주면, status가 PENDING이고 eventId가 UUID로 채워진다.")
        @Test
        void creates_withPendingStatusAndEventId_whenValidInfoIsProvided() {
            OutboxEvent event = new OutboxEvent(
                "OrderItemSoldEvent",
                "{\"orderId\":1}",
                "catalog-events-v1",
                "1"
            );

            assertAll(
                () -> assertThat(event.getEventId()).isNotNull(),
                () -> assertThat(event.getEventType()).isEqualTo("OrderItemSoldEvent"),
                () -> assertThat(event.getTopicName()).isEqualTo("catalog-events-v1"),
                () -> assertThat(event.getPartitionKey()).isEqualTo("1"),
                () -> assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING),
                () -> assertThat(event.getRetryCount()).isEqualTo(0),
                () -> assertThat(event.getPublishedAt()).isNull()
            );
        }

        @DisplayName("eventType이 null이면, CoreException이 발생한다.")
        @Test
        void throws_whenEventTypeIsNull() {
            assertThrows(CoreException.class, () ->
                new OutboxEvent(null, "{}", "catalog-events-v1", "1")
            );
        }

        @DisplayName("topicName이 null이면, CoreException이 발생한다.")
        @Test
        void throws_whenTopicNameIsNull() {
            assertThrows(CoreException.class, () ->
                new OutboxEvent("OrderItemSoldEvent", "{}", null, "1")
            );
        }

        @DisplayName("partitionKey가 null이면, CoreException이 발생한다.")
        @Test
        void throws_whenPartitionKeyIsNull() {
            assertThrows(CoreException.class, () ->
                new OutboxEvent("OrderItemSoldEvent", "{}", "catalog-events-v1", null)
            );
        }
    }

    @DisplayName("markPublished()를 호출하면,")
    @Nested
    class MarkPublished {

        @DisplayName("status가 PUBLISHED로 변경되고 publishedAt이 설정된다.")
        @Test
        void changesStatusToPublished() {
            OutboxEvent event = new OutboxEvent("OrderItemSoldEvent", "{}", "catalog-events-v1", "1");

            event.markPublished();

            assertAll(
                () -> assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED),
                () -> assertThat(event.getPublishedAt()).isNotNull()
            );
        }
    }

    @DisplayName("markFailed()를 호출하면,")
    @Nested
    class MarkFailed {

        @DisplayName("status가 FAILED로 변경되고 retryCount가 1 증가하고 errorMessage가 설정된다.")
        @Test
        void changesStatusToFailed() {
            OutboxEvent event = new OutboxEvent("OrderItemSoldEvent", "{}", "catalog-events-v1", "1");

            event.markFailed("connection timeout");

            assertAll(
                () -> assertThat(event.getStatus()).isEqualTo(OutboxStatus.FAILED),
                () -> assertThat(event.getRetryCount()).isEqualTo(1),
                () -> assertThat(event.getErrorMessage()).isEqualTo("connection timeout")
            );
        }
    }
}
