package com.loopers.application.ranking;

import com.loopers.application.product.ProductService;
import com.loopers.domain.product.Product;
import com.loopers.domain.ranking.RankingItem;
import com.loopers.domain.ranking.RankingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Component
public class RankingFacade {

    private final RankingRepository rankingRepository;
    private final ProductService productService;
    private final Clock clock;

    /**
     * 랭킹 페이지를 상품정보와 함께 조회한다.
     *
     * @param date 조회 날짜 (null이면 오늘)
     * @param page 0-based 페이지 번호
     */
    public RankingInfo getRankings(LocalDate date, int page, int size) {
        LocalDate targetDate = date != null ? date : LocalDate.now(clock);
        List<RankingItem> rankingItems = rankingRepository.findPage(targetDate, page, size);
        long totalCount = rankingRepository.countTotal(targetDate);
        if (rankingItems.isEmpty()) {
            return new RankingInfo(targetDate, totalCount, List.of());
        }

        List<Long> productIds = rankingItems.stream().map(RankingItem::productId).toList();
        Map<Long, Product> products = productService.getProductsByIds(productIds).stream()
            .collect(Collectors.toMap(Product::getId, Function.identity()));

        long offset = (long) page * size;
        List<RankingInfo.RankingProductInfo> items = new ArrayList<>();
        for (int i = 0; i < rankingItems.size(); i++) {
            RankingItem rankingItem = rankingItems.get(i);
            Product product = products.get(rankingItem.productId());
            if (product == null) {
                continue; // 삭제된 상품은 제외 (순위는 ZSET 기준 유지)
            }
            items.add(RankingInfo.RankingProductInfo.of(offset + i + 1, product, rankingItem.score()));
        }
        return new RankingInfo(targetDate, totalCount, items);
    }
}
