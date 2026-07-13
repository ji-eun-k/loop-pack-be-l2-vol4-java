package com.loopers.interfaces.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.ranking.RankingScorePolicy;
import com.loopers.domain.ranking.RankingEventScore;
import com.loopers.domain.ranking.RankingWeightProperties;
import com.loopers.application.ranking.RankingEventDateResolver;
import com.loopers.infrastructure.catalog.ProductMetricsJpaRepository;
import com.loopers.infrastructure.ranking.RankingUpdater;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.Map;
import java.time.Clock;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class CatalogViewConsumerTest {

    @Mock
    private ProductMetricsJpaRepository productMetricsJpaRepository;

    @Mock
    private DlqPublisher dlqPublisher;

    @Mock
    private RankingUpdater rankingUpdater;

    private CatalogViewConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new CatalogViewConsumer(productMetricsJpaRepository, new ObjectMapper(), dlqPublisher,
            new RankingScorePolicy(new RankingWeightProperties(0.1, 0.2, 0.6)), rankingUpdater,
            new RankingEventDateResolver(Clock.systemUTC()));
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

        @DisplayName("배치 내 조회 이벤트를 상품별로 집계해 랭킹 델타를 한 번에 반영한다.")
        @Test
        void aggregatesViewDeltas_andAppliesOncePerBatch() {
            Acknowledgment ack = mock(Acknowledgment.class);
            List<ConsumerRecord<Object, Object>> records = List.of(
                new ConsumerRecord<>("catalog-view-events-v1", 0, 0L, null, "{\"productId\":100}"),
                new ConsumerRecord<>("catalog-view-events-v1", 0, 1L, null, "{\"productId\":100}"),
                new ConsumerRecord<>("catalog-view-events-v1", 0, 2L, null, "{\"productId\":100}"),
                new ConsumerRecord<>("catalog-view-events-v1", 0, 3L, null, "{\"productId\":200}")
            );

            consumer.consume(records, ack);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<List<RankingEventScore>> captor = ArgumentCaptor.forClass(List.class);
            then(rankingUpdater).should().applyEvents(org.mockito.ArgumentMatchers.any(LocalDate.class), captor.capture());
            double product100Score = captor.getValue().stream()
                .mapToDouble(event -> event.deltas().getOrDefault(100L, 0.0)).sum();
            double product200Score = captor.getValue().stream()
                .mapToDouble(event -> event.deltas().getOrDefault(200L, 0.0)).sum();
            assertThat(product100Score).isCloseTo(0.3, within(1e-9));
            assertThat(product200Score).isCloseTo(0.1, within(1e-9));
        }

        @DisplayName("파싱에 실패한 레코드는 랭킹 델타 집계에서 제외된다.")
        @Test
        void excludesInvalidRecords_fromRankingDeltas() {
            Acknowledgment ack = mock(Acknowledgment.class);
            List<ConsumerRecord<Object, Object>> records = List.of(
                new ConsumerRecord<>("catalog-view-events-v1", 0, 0L, null, "{\"productId\":100}"),
                new ConsumerRecord<>("catalog-view-events-v1", 0, 1L, null, "invalid-json")
            );

            consumer.consume(records, ack);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<List<RankingEventScore>> captor = ArgumentCaptor.forClass(List.class);
            then(rankingUpdater).should().applyEvents(org.mockito.ArgumentMatchers.any(LocalDate.class), captor.capture());
            assertThat(captor.getValue()).hasSize(1);
            assertThat(captor.getValue().get(0).deltas().get(100L)).isCloseTo(0.1, within(1e-9));
        }

        @DisplayName("Redis 랭킹 반영이 실패하면 offset을 커밋하지 않고 예외를 전파한다.")
        @Test
        void doesNotAcknowledge_whenRankingUpdateFails() {
            Acknowledgment ack = mock(Acknowledgment.class);
            ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "catalog-view-events-v1", 0, 0L, null, "{\"productId\":100}"
            );
            doThrow(new IllegalStateException("redis unavailable"))
                .when(rankingUpdater).applyEvents(org.mockito.ArgumentMatchers.any(LocalDate.class), org.mockito.ArgumentMatchers.anyList());

            assertThrows(IllegalStateException.class, () -> consumer.consume(List.of(record), ack));

            then(ack).should(never()).acknowledge();
        }
    }
}
