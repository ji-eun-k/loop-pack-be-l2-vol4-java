package com.loopers.domain.ranking;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

public interface RankingRepository {

    /**
     * 해당 날짜의 랭킹을 점수 내림차순으로 페이지 조회한다.
     *
     * @param page 0-based 페이지 번호
     */
    List<RankingItem> findPage(LocalDate date, int page, int size);

    /**
     * 해당 날짜의 상품 순위를 반환한다 (1-based). 순위권 밖이면 빈 Optional.
     */
    Optional<Long> findRank(LocalDate date, Long productId);

    long countTotal(LocalDate date);
}
