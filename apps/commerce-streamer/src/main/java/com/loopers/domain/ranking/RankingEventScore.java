package com.loopers.domain.ranking;

import java.util.Map;

/**
 * 이벤트 하나가 유발하는 상품별 랭킹 점수 증감치. eventId는 RankingUpdater가 멱등 처리(중복 반영 방지)에 사용한다.
 */
public record RankingEventScore(String eventId, Map<Long, Double> deltas) {

    public RankingEventScore {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        // 방어적 복사로 외부에서 deltas를 변경해도 이 레코드의 불변성이 깨지지 않게 한다.
        deltas = Map.copyOf(deltas);
    }
}
