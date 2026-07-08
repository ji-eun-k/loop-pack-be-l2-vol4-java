package com.loopers.application.queue;

import com.loopers.domain.queue.EntryTokenRepository;
import com.loopers.domain.queue.QueuePositionSnapshot;
import com.loopers.domain.queue.QueueRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class QueueServiceStreamPositionTest {

    @Mock private QueueRepository queueRepository;
    @Mock private EntryTokenRepository entryTokenRepository;
    @Mock private QueueProperties queueProperties;
    @Mock private SseEmitterRegistry sseEmitterRegistry;

    @InjectMocks private QueueService queueService;

    @DisplayName("streamPosition() 을 호출하면,")
    @Nested
    class StreamPosition {

        @DisplayName("SseEmitter를 반환한다.")
        @Test
        void returnsSseEmitter() {
            // arrange
            given(entryTokenRepository.find(1L)).willReturn(Optional.of("token"));

            // act
            SseEmitter emitter = queueService.streamPosition(1L);

            // assert
            assertThat(emitter).isNotNull();
        }

        @DisplayName("ACTIVE 상태이면 registry에 등록하지 않는다.")
        @Test
        void doesNotRegister_whenActive() {
            // arrange
            given(entryTokenRepository.find(1L)).willReturn(Optional.of("token"));

            // act
            queueService.streamPosition(1L);

            // assert
            verify(sseEmitterRegistry, never()).register(any(), any());
        }

        @DisplayName("NOT_IN_QUEUE 상태이면 registry에 등록하지 않는다.")
        @Test
        void doesNotRegister_whenNotInQueue() {
            // arrange
            given(entryTokenRepository.find(1L)).willReturn(Optional.empty());
            given(queueRepository.findPositionSnapshot(1L)).willReturn(Optional.empty());

            // act
            queueService.streamPosition(1L);

            // assert
            verify(sseEmitterRegistry, never()).register(any(), any());
        }

        @DisplayName("WAITING 상태이면 registry에 emitter를 등록한다.")
        @Test
        void registersEmitter_whenWaiting() {
            // arrange
            given(entryTokenRepository.find(1L)).willReturn(Optional.empty());
            given(queueRepository.findPositionSnapshot(1L))
                    .willReturn(Optional.of(new QueuePositionSnapshot(5L, 100L)));
            given(queueProperties.getBatchSize()).willReturn(75);
            given(queueProperties.getSchedulerIntervalMs()).willReturn(1000L);

            // act
            queueService.streamPosition(1L);

            // assert
            verify(sseEmitterRegistry).register(eq(1L), any(SseEmitter.class));
        }
    }
}
