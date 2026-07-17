package com.loopers.domain.catalog;

import java.time.Instant;

/**
 * catalog_event_ledger 테이블에 저장되는 원장 이벤트.
 * Kafka에서 소비한 카탈로그 이벤트를 먼저 원장에 적재해두면, product_metrics/랭킹 반영이
 * 실패하거나 Redis가 유실돼도 이 원장을 재생(replay)해 복구할 수 있다.
 */
public record CatalogLedgerEvent(
    String eventId,
    String eventType,
    Instant occurredAt,
    Instant receivedAt,
    String payload,
    String sourceTopic,
    int sourcePartition,
    long sourceOffset,
    int eventVersion
) {
}