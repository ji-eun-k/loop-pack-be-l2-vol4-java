package com.loopers.application.product;

import com.loopers.domain.product.Product;
import com.loopers.domain.product.ProductRepository;
import com.loopers.domain.product.ProductSort;
import com.loopers.domain.product.ProductStock;
import com.loopers.domain.product.ProductStockRepository;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Slf4j
@RequiredArgsConstructor
@Component
public class ProductService {

    static final String LIKE_PENDING_KEY_PREFIX = "product:like:pending:";

    private final ProductRepository productRepository;
    private final ProductStockRepository productStockRepository;
    private final RedisTemplate<String, String> redisTemplate;

    public Product getProduct(Long id) {
        return productRepository.find(id)
            .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "존재하지 않는 상품입니다."));
    }

    public ProductStock getProductStock(Long productId) {
        return productStockRepository.findByProductId(productId)
            .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "존재하지 않는 상품입니다."));
    }

    public Page<Product> getProducts(Long brandId, ProductSort sort, Pageable pageable) {
        Pageable p = PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), toSort(sort));
        return productRepository.findAll(brandId, p);
    }

    private Sort toSort(ProductSort sort) {
        return switch (sort) {
            case PRICE_ASC -> Sort.by(Sort.Direction.ASC, "price");
            case LIKES_DESC -> Sort.by(Sort.Direction.DESC, "likeCount");
            default -> Sort.by(Sort.Direction.DESC, "createdAt");
        };
    }

    @Transactional
    public Product createProduct(Long brandId, String name, BigDecimal price, long stock) {
        Product product = new Product(brandId, name, price);
        Product saved = productRepository.save(product);
        productStockRepository.save(new ProductStock(saved.getId(), stock));
        return saved;
    }

    @Transactional
    public Product updateProduct(Long id, Long brandId, String name, BigDecimal price, long stock) {
        Product product = getProduct(id);
        product.update(brandId, name, price);
        ProductStock productStock = getProductStock(id);
        productStock.updateQuantity(stock);
        productStockRepository.save(productStock);
        return productRepository.save(product);
    }

    @Transactional
    public void deleteProduct(Long id) {
        Product product = getProduct(id);
        product.delete();
        productRepository.save(product);
    }

    @Transactional
    public void deductStock(Long productId, long quantity) {
        ProductStock stock = productStockRepository.findByProductIdForUpdate(productId);
        stock.deduct(quantity);
        productStockRepository.save(stock);
    }

    public void incrementLikeCount(Long productId) {
        try {
            redisTemplate.opsForValue().increment(LIKE_PENDING_KEY_PREFIX + productId);
        } catch (Exception e) {
            log.warn("Redis likeCount increment 실패, productId={}", productId, e);
        }
    }

    public void decrementLikeCount(Long productId) {
        try {
            redisTemplate.opsForValue().decrement(LIKE_PENDING_KEY_PREFIX + productId);
        } catch (Exception e) {
            log.warn("Redis likeCount decrement 실패, productId={}", productId, e);
        }
    }

    public void deleteAllByBrandId(Long brandId) {
        productRepository.deleteAllByBrandId(brandId);
    }
}
