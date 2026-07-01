package com.loopers.infrastructure.outbox;

import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@RequiredArgsConstructor
@Component
public class OutboxRepositoryImpl implements OutboxRepository {

    private final OutboxJpaRepository outboxJpaRepository;

    @Override
    public OutboxEvent save(OutboxEvent event) {
        if (event.getId() != null) {
            OutboxEventEntity entity = outboxJpaRepository.findById(event.getId())
                .orElseThrow();
            entity.updateFrom(event);
            return entity.toDomain();
        }
        return outboxJpaRepository.save(new OutboxEventEntity(
            event.getEventType(), event.getPayload(),
            event.getTopicName(), event.getPartitionKey(), event.getIdempotencyKey()
        )).toDomain();
    }

    @Override
    public List<OutboxEvent> findPending() {
        return outboxJpaRepository.findPendingWithSkipLocked().stream()
            .map(OutboxEventEntity::toDomain)
            .toList();
    }
}
