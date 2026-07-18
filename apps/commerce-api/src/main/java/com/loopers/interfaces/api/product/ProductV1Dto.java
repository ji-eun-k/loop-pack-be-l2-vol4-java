package com.loopers.interfaces.api.product;

import com.loopers.application.product.ProductDetailInfo;
import com.loopers.application.product.ProductInfo;

import java.math.BigDecimal;

public class ProductV1Dto {

    public record ProductResponse(
        Long id,
        Long brandId,
        String name,
        BigDecimal price,
        long likeCount
    ) {
        public static ProductResponse from(ProductInfo info) {
            return new ProductResponse(info.id(), info.brandId(), info.name(), info.price(), info.likeCount());
        }
    }

    public record ProductDetailResponse(
        Long id,
        Long brandId,
        String name,
        BigDecimal price,
        long likeCount,
        Long rank
    ) {
        public static ProductDetailResponse from(ProductDetailInfo info) {
            return new ProductDetailResponse(
                info.id(), info.brandId(), info.name(), info.price(), info.likeCount(), info.rank()
            );
        }
    }
}
