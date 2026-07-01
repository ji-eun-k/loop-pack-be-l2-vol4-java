package com.loopers.interfaces.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.infrastructure.catalog.ProductMetricsJpaRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
class CatalogViewConsumerTest {

    @Mock
    private ProductMetricsJpaRepository productMetricsJpaRepository;

    private CatalogViewConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new CatalogViewConsumer(productMetricsJpaRepository, new ObjectMapper());
    }

    @DisplayName("consume()을 실행할 때,")
    @Nested
    class Consume {

        @DisplayName("ProductViewedEvent 수신 시 view_count를 증가시킨다.")
        @Test
        void incrementsViewCount_whenProductViewedPayloadReceived() {
            Acknowledgment ack = mock(Acknowledgment.class);
            ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "catalog-view-events-v1", 0, 0L, null, "{\"productId\":100}"
            );

            consumer.consume(List.of(record), ack);

            then(productMetricsJpaRepository).should().upsertViewCountIncrement(100L);
            then(ack).should().acknowledge();
        }

        @DisplayName("payload 파싱 실패 시 예외 없이 ack을 커밋한다.")
        @Test
        void acknowledgesWithoutException_whenPayloadInvalid() {
            Acknowledgment ack = mock(Acknowledgment.class);
            ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "catalog-view-events-v1", 0, 0L, null, "invalid-json"
            );

            consumer.consume(List.of(record), ack);

            then(productMetricsJpaRepository).should(never()).upsertViewCountIncrement(100L);
            then(ack).should().acknowledge();
        }
    }
}
