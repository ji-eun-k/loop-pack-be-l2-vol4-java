package com.loopers.application.outbox;

import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxRepository;
import com.loopers.domain.outbox.OutboxStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
class OutboxEventPublishSchedulerTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private OutboxEventPublishScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new OutboxEventPublishScheduler(outboxRepository, kafkaTemplate);
    }

    @DisplayName("publish()를 실행할 때,")
    @Nested
    class Publish {

        @DisplayName("PENDING 이벤트가 없으면 아무것도 하지 않는다.")
        @Test
        void doesNothing_whenNoPendingEvents() {
            given(outboxRepository.findPending()).willReturn(List.of());

            scheduler.publish();

            then(kafkaTemplate).should(never()).send(anyString(), any(), anyString());
        }

        @DisplayName("Kafka 발행 성공 시 status가 PUBLISHED로 변경된다.")
        @Test
        void marksPublished_whenKafkaSendSucceeds() {
            OutboxEvent event = new OutboxEvent("OrderItemSoldEvent", "{}", "catalog-events-v1", "1", "1");
            given(outboxRepository.findPending()).willReturn(List.of(event));
            given(kafkaTemplate.send(anyString(), any(), anyString()))
                .willReturn(CompletableFuture.completedFuture(null));

            scheduler.publish();

            assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
            assertThat(event.getPublishedAt()).isNotNull();
        }

        @DisplayName("Kafka 발행 실패 시 status가 FAILED로 변경되고 retryCount가 증가한다.")
        @Test
        void marksFailedAndIncrementsRetryCount_whenKafkaSendFails() {
            OutboxEvent event = new OutboxEvent("OrderItemSoldEvent", "{}", "catalog-events-v1", "1", "1");
            given(outboxRepository.findPending()).willReturn(List.of(event));
            given(kafkaTemplate.send(anyString(), any(), anyString()))
                .willReturn(CompletableFuture.failedFuture(new RuntimeException("connection error")));

            scheduler.publish();

            assertThat(event.getStatus()).isEqualTo(OutboxStatus.FAILED);
            assertThat(event.getRetryCount()).isEqualTo(1);
            assertThat(event.getErrorMessage()).isNotNull();
        }

        @DisplayName("발행 성공/실패 여부와 무관하게 save()를 호출한다.")
        @Test
        void alwaysCallsSave_regardlessOfKafkaResult() {
            OutboxEvent event = new OutboxEvent("OrderItemSoldEvent", "{}", "catalog-events-v1", "1", "1");
            given(outboxRepository.findPending()).willReturn(List.of(event));
            given(kafkaTemplate.send(anyString(), any(), anyString()))
                .willReturn(CompletableFuture.completedFuture(null));

            scheduler.publish();

            then(outboxRepository).should().save(event);
        }
    }
}
