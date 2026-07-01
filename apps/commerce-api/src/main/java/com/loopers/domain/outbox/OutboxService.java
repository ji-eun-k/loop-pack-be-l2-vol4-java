package com.loopers.domain.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@RequiredArgsConstructor
@Component
public class OutboxService {

    private final OutboxRepository outboxRepository;

    @Transactional
    public void save(OutboxEvent event) {
        outboxRepository.save(event);
    }
}
