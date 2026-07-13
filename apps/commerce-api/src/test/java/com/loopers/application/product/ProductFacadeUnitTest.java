package com.loopers.application.product;

import com.loopers.application.brand.BrandService;
import com.loopers.application.event.UserActionEvent;
import com.loopers.application.event.UserActionType;
import com.loopers.domain.product.Product;
import com.loopers.domain.ranking.RankingRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

@ExtendWith(MockitoExtension.class)
class ProductFacadeUnitTest {

    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");
    private static final LocalDate TODAY = LocalDate.of(2026, 7, 12);

    @Mock
    private ProductService productService;

    @Mock
    private BrandService brandService;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    @Mock
    private RankingRepository rankingRepository;

    private ProductFacade productFacade;

    @BeforeEach
    void setUp() {
        Clock clock = Clock.fixed(TODAY.atStartOfDay(ZONE).toInstant(), ZONE);
        productFacade = new ProductFacade(productService, brandService, eventPublisher, rankingRepository, clock);
    }

    private Product product(Long id) {
        return new Product(id, 1L, "상품", BigDecimal.valueOf(10_000), 5L, null, null, null);
    }

    @DisplayName("getProductDetail()로 상품 상세를 조회할 때,")
    @Nested
    class GetProductDetail {

        @DisplayName("PRODUCT_VIEWED 이벤트를 발행한다.")
        @Test
        void publishesProductViewedEvent() {
            Long productId = 10L;
            given(productService.getProduct(productId)).willReturn(product(productId));
            given(rankingRepository.findRank(TODAY, productId)).willReturn(Optional.empty());

            productFacade.getProductDetail(productId);

            ArgumentCaptor<UserActionEvent> captor = ArgumentCaptor.forClass(UserActionEvent.class);
            then(eventPublisher).should().publishEvent(captor.capture());
            assertAll(
                () -> assertThat(captor.getValue().actionType()).isEqualTo(UserActionType.PRODUCT_VIEWED),
                () -> assertThat(captor.getValue().resourceId()).isEqualTo(productId)
            );
        }

        @DisplayName("오늘 랭킹에 있으면, 순위를 포함해 반환한다.")
        @Test
        void returnsRank_whenProductIsRankedToday() {
            Long productId = 10L;
            given(productService.getProduct(productId)).willReturn(product(productId));
            given(rankingRepository.findRank(TODAY, productId)).willReturn(Optional.of(3L));

            ProductDetailInfo info = productFacade.getProductDetail(productId);

            assertAll(
                () -> assertThat(info.id()).isEqualTo(productId),
                () -> assertThat(info.rank()).isEqualTo(3L)
            );
        }

        @DisplayName("오늘 랭킹에 없으면, 순위는 null이다.")
        @Test
        void returnsNullRank_whenProductIsNotRanked() {
            Long productId = 10L;
            given(productService.getProduct(productId)).willReturn(product(productId));
            given(rankingRepository.findRank(TODAY, productId)).willReturn(Optional.empty());

            ProductDetailInfo info = productFacade.getProductDetail(productId);

            assertThat(info.rank()).isNull();
        }
    }
}
