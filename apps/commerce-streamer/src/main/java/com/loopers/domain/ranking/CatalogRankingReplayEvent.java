package com.loopers.domain.ranking;

import java.time.Instant;

/**
 * 랭킹 리커버리 시 원장에서 조회한 리플레이 대상 이벤트.
 * ledgerId는 배치 커서로 사용되며(RankingRecoveryService 참고), Kafka offset과는 무관한
 * 원장 테이블 자체의 auto-increment PK다.
 */
public record CatalogRankingReplayEvent(
    long ledgerId,
    String eventId,
    String eventType,
    Instant occurredAt,
    String payload
) {
}