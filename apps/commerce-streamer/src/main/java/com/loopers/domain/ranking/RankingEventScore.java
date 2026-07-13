package com.loopers.domain.ranking;

import java.util.Map;

public record RankingEventScore(String eventId, Map<Long, Double> deltas) {

    public RankingEventScore {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        deltas = Map.copyOf(deltas);
    }
}
