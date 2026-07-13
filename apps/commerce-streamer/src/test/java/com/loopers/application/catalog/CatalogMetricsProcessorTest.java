package com.loopers.application.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.ranking.RankingScorePolicy;
import com.loopers.domain.ranking.RankingWeightProperties;
import com.loopers.infrastructure.catalog.EventHandledEntity;
import com.loopers.infrastructure.catalog.EventHandledJpaRepository;
import com.loopers.infrastructure.catalog.ProductMetricsJpaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
class CatalogMetricsProcessorTest {

    @Mock
    private EventHandledJpaRepository eventHandledJpaRepository;

    @Mock
    private ProductMetricsJpaRepository productMetricsJpaRepository;

    private CatalogMetricsProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new CatalogMetricsProcessor(eventHandledJpaRepository, productMetricsJpaRepository,
            new ObjectMapper(), new RankingScorePolicy(new RankingWeightProperties(0.1, 0.2, 0.6)));
    }

    @DisplayName("process()를 실행할 때,")
    @Nested
    class Process {

        @DisplayName("이미 처리된 eventId이면 metrics를 업데이트하지 않는다.")
        @Test
        void skips_whenEventAlreadyHandled() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-1")).willReturn(true);

            processor.process("OrderItemSoldEvent", "uuid-1", "catalog-events-v1",
                "{\"orderId\":1,\"productQtyMap\":{\"10\":2}}");

            then(productMetricsJpaRepository).should(never()).upsertOrderCount(anyLong(), anyInt());
            then(eventHandledJpaRepository).should(never()).save(any());
        }

        @DisplayName("OrderItemSoldEvent 처리 시 productQtyMap 기준으로 order_count를 upsert한다.")
        @Test
        void upsertsOrderCount_whenOrderItemSoldEvent() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-2")).willReturn(false);

            processor.process("OrderItemSoldEvent", "uuid-2", "catalog-events-v1",
                "{\"orderId\":1,\"productQtyMap\":{\"10\":2,\"20\":3}}");

            then(productMetricsJpaRepository).should().upsertOrderCount(10L, 2);
            then(productMetricsJpaRepository).should().upsertOrderCount(20L, 3);
        }

        @DisplayName("ProductLikedEvent 처리 시 like_count를 1 증가시킨다.")
        @Test
        void incrementsLikeCount_whenProductLikedEvent() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-3")).willReturn(false);

            processor.process("ProductLikedEvent", "uuid-3", "catalog-events-v1",
                "{\"productId\":10}");

            then(productMetricsJpaRepository).should().upsertLikeCountIncrement(10L);
        }

        @DisplayName("ProductUnlikedEvent 처리 시 like_count를 1 감소시킨다.")
        @Test
        void decrementsLikeCount_whenProductUnlikedEvent() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-4")).willReturn(false);

            processor.process("ProductUnlikedEvent", "uuid-4", "catalog-events-v1",
                "{\"productId\":10}");

            then(productMetricsJpaRepository).should().upsertLikeCountDecrement(10L);
        }

        @DisplayName("처리 완료 후 eventHandled를 저장한다.")
        @Test
        void savesEventHandled_afterProcessing() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-5")).willReturn(false);

            processor.process("ProductLikedEvent", "uuid-5", "catalog-events-v1",
                "{\"productId\":10}");

            ArgumentCaptor<EventHandledEntity> captor = ArgumentCaptor.forClass(EventHandledEntity.class);
            then(eventHandledJpaRepository).should().save(captor.capture());
            assertThat(captor.getValue().getEventId()).isEqualTo("uuid-5");
            assertThat(captor.getValue().getTopic()).isEqualTo("catalog-events-v1");
        }

        @DisplayName("알 수 없는 이벤트 타입이면 예외 없이 eventHandled만 저장한다.")
        @Test
        void savesEventHandledOnly_whenUnknownEventType() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-6")).willReturn(false);

            processor.process("UnknownEvent", "uuid-6", "catalog-events-v1", "{}");

            then(productMetricsJpaRepository).shouldHaveNoInteractions();
            then(eventHandledJpaRepository).should().save(any());
        }
    }

    @DisplayName("랭킹 델타를 반환할 때,")
    @Nested
    class RankingDeltas {

        @DisplayName("OrderItemSoldEvent는 productAmountMap 기준으로 상품별 주문 점수 델타를 반환한다.")
        @Test
        void returnsOrderDeltas_whenOrderItemSoldEventHasAmountMap() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-10")).willReturn(false);

            Map<Long, Double> deltas = processor.process("OrderItemSoldEvent", "uuid-10", "catalog-events-v1",
                "{\"orderId\":1,\"productQtyMap\":{\"10\":2,\"20\":3},\"productAmountMap\":{\"10\":24000,\"20\":15000}}");

            assertThat(deltas.get(10L)).isCloseTo(0.6 * Math.log10(1 + 24_000), within(1e-9));
            assertThat(deltas.get(20L)).isCloseTo(0.6 * Math.log10(1 + 15_000), within(1e-9));
        }

        @DisplayName("OrderItemSoldEvent에 productAmountMap이 없으면(구버전 이벤트) 빈 델타를 반환한다.")
        @Test
        void returnsEmptyDeltas_whenAmountMapMissing() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-11")).willReturn(false);

            Map<Long, Double> deltas = processor.process("OrderItemSoldEvent", "uuid-11", "catalog-events-v1",
                "{\"orderId\":1,\"productQtyMap\":{\"10\":2}}");

            assertThat(deltas).isEmpty();
        }

        @DisplayName("ProductLikedEvent는 like 가중치 델타를 반환한다.")
        @Test
        void returnsLikeDelta_whenProductLikedEvent() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-12")).willReturn(false);

            Map<Long, Double> deltas = processor.process("ProductLikedEvent", "uuid-12", "catalog-events-v1",
                "{\"productId\":10}");

            assertThat(deltas).containsEntry(10L, 0.2);
        }

        @DisplayName("ProductUnlikedEvent는 like 가중치의 음수 델타를 반환한다.")
        @Test
        void returnsNegativeLikeDelta_whenProductUnlikedEvent() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-13")).willReturn(false);

            Map<Long, Double> deltas = processor.process("ProductUnlikedEvent", "uuid-13", "catalog-events-v1",
                "{\"productId\":10}");

            assertThat(deltas).containsEntry(10L, -0.2);
        }

        @DisplayName("중복 eventId여도 Redis 실패 재처리를 위해 점수 델타를 반환한다.")
        @Test
        void returnsRankingDeltas_whenEventAlreadyHandled() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-14")).willReturn(true);

            Map<Long, Double> deltas = processor.process("ProductLikedEvent", "uuid-14", "catalog-events-v1",
                "{\"productId\":10}");

            assertThat(deltas).containsEntry(10L, 0.2);
        }

        @DisplayName("알 수 없는 이벤트 타입이면 빈 델타를 반환한다.")
        @Test
        void returnsEmptyDeltas_whenUnknownEventType() throws Exception {
            given(eventHandledJpaRepository.existsByEventId("uuid-15")).willReturn(false);

            Map<Long, Double> deltas = processor.process("UnknownEvent", "uuid-15", "catalog-events-v1", "{}");

            assertThat(deltas).isEmpty();
        }
    }
}
