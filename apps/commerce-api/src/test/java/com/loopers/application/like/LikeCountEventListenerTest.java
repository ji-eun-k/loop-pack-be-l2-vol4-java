package com.loopers.application.like;

import com.loopers.application.product.ProductService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willThrow;

@ExtendWith(MockitoExtension.class)
class LikeCountEventListenerTest {

    @Mock
    private ProductService productService;

    private LikeCountEventListener listener;

    @BeforeEach
    void setUp() {
        listener = new LikeCountEventListener(productService);
    }

    @DisplayName("좋아요 이벤트를 수신할 때,")
    @Nested
    class Handle {

        @DisplayName("increase=true이면 incrementLikeCount를 호출한다.")
        @Test
        void callsIncrement_whenIncreaseIsTrue() {
            listener.handle(new LikeCountChangedEvent(10L, true));

            then(productService).should().incrementLikeCount(10L);
            then(productService).shouldHaveNoMoreInteractions();
        }

        @DisplayName("increase=false이면 decrementLikeCount를 호출한다.")
        @Test
        void callsDecrement_whenIncreaseIsFalse() {
            listener.handle(new LikeCountChangedEvent(10L, false));

            then(productService).should().decrementLikeCount(10L);
            then(productService).shouldHaveNoMoreInteractions();
        }

        @DisplayName("집계 업데이트 중 예외가 발생해도 리스너는 예외를 전파하지 않는다.")
        @Test
        void doesNotThrow_whenProductServiceFails() {
            willThrow(new RuntimeException("Redis 장애")).given(productService).incrementLikeCount(10L);

            assertThatCode(() -> listener.handle(new LikeCountChangedEvent(10L, true)))
                .doesNotThrowAnyException();
        }
    }
}