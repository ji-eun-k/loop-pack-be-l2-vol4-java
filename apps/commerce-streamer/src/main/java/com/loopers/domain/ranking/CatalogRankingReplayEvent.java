package com.loopers.domain.ranking;

import java.time.Instant;

public record CatalogRankingReplayEvent(
    long ledgerId,
    String eventId,
    String eventType,
    Instant occurredAt,
    String payload
) {
}