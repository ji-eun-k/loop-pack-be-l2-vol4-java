package com.loopers.application.product;

import com.loopers.domain.product.Product;

import java.math.BigDecimal;

public record ProductDetailInfo(
    Long id,
    Long brandId,
    String name,
    BigDecimal price,
    long likeCount,
    Long rank
) {
    public static ProductDetailInfo of(Product product, Long rank) {
        return new ProductDetailInfo(
            product.getId(), product.getBrandId(), product.getName(), product.getPrice(),
            product.getLikeCount(), rank
        );
    }
}
