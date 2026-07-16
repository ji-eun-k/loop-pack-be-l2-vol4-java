package com.loopers.domain.catalog;

import java.time.Instant;

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