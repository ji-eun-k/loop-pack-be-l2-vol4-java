package com.loopers.application.queue;

import com.loopers.domain.queue.QueueStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SseEmitterRegistryTest {

    private SseEmitterRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SseEmitterRegistry();
    }

    @DisplayName("register() 를 호출하면,")
    @Nested
    class Register {

        @DisplayName("해당 userId가 등록된 목록에 포함된다.")
        @Test
        void includesUserIdInRegisteredSet() {
            // arrange
            SseEmitter emitter = new SseEmitter();

            // act
            registry.register(1L, emitter);

            // assert
            assertThat(registry.getRegisteredUserIds()).contains(1L);
        }
    }

    @DisplayName("sendActive() 를 호출하면,")
    @Nested
    class SendActive {

        @DisplayName("등록된 emitter가 없으면 false를 반환한다.")
        @Test
        void returnsFalse_whenEmitterNotRegistered() {
            // act & assert
            assertThat(registry.sendActive(99L, "token")).isFalse();
        }

        @DisplayName("등록된 emitter가 있으면 true를 반환한다.")
        @Test
        void returnsTrue_whenEmitterRegistered() {
            // arrange
            SseEmitter emitter = new SseEmitter();
            registry.register(1L, emitter);

            // act & assert
            assertThat(registry.sendActive(1L, "token")).isTrue();
        }
    }

    @DisplayName("sendPosition() 을 호출하면,")
    @Nested
    class SendPosition {

        @DisplayName("처음 호출 시 emitter에 이벤트를 전송한다.")
        @Test
        void sendsOnFirstCall() throws Exception {
            // arrange
            SseEmitter mockEmitter = mock(SseEmitter.class);
            registry.register(1L, mockEmitter);
            QueuePosition position = new QueuePosition(QueueStatus.WAITING, 30, 100, 1_000, 1, null);

            // act
            registry.sendPosition(1L, position);

            // assert
            verify(mockEmitter).send(any(SseEmitter.SseEventBuilder.class));
        }

        @DisplayName("재연결 후 이전 emitter의 종료 콜백이 늦게 실행되어도, 새 연결은 PUSH를 받는다.")
        @Test
        void sendsToNewEmitter_whenStaleEmitterCleanedUpAfterReconnect() throws Exception {
            // arrange - 최초 연결 후 재연결로 emitter가 교체된 상태
            SseEmitter oldEmitter = mock(SseEmitter.class);
            SseEmitter newEmitter = mock(SseEmitter.class);
            ArgumentCaptor<Runnable> completionCaptor = ArgumentCaptor.forClass(Runnable.class);
            registry.register(1L, oldEmitter);
            verify(oldEmitter).onCompletion(completionCaptor.capture());
            registry.register(1L, newEmitter);

            // act - 이전 emitter의 종료 콜백이 뒤늦게 도착한 뒤 순번 PUSH
            completionCaptor.getValue().run();
            QueuePosition position = new QueuePosition(QueueStatus.WAITING, 30, 100, 1_000, 1, null);
            registry.sendPosition(1L, position);

            // assert
            verify(newEmitter).send(any(SseEmitter.SseEventBuilder.class));
        }

        @DisplayName("nextPollAfterMs 이내에 재호출하면 전송을 스킵한다.")
        @Test
        void skipsWhenCalledBeforeNextPushTime() throws Exception {
            // arrange
            SseEmitter mockEmitter = mock(SseEmitter.class);
            registry.register(1L, mockEmitter);
            // nextPollAfterMs = 3000ms → 두 번째 즉시 호출은 스킵되어야 한다
            QueuePosition position = new QueuePosition(QueueStatus.WAITING, 200, 500, 3_000, 3, null);

            // act
            registry.sendPosition(1L, position);
            registry.sendPosition(1L, position);

            // assert - 첫 번째만 전송, 두 번째는 스킵
            verify(mockEmitter, times(1)).send(any(SseEmitter.SseEventBuilder.class));
        }
    }
}
