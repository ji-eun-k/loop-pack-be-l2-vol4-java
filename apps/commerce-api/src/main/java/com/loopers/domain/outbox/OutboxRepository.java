package com.loopers.domain.outbox;

import java.util.List;

public interface OutboxRepository {
    OutboxEvent save(OutboxEvent event);
    List<OutboxEvent> findPending();
}
