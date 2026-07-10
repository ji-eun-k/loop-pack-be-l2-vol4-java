package com.loopers.application.order;

import com.loopers.domain.queue.EntryTokenRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@RequiredArgsConstructor
public class QueueTokenCleanupListener {

    private final EntryTokenRepository entryTokenRepository;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handle(OrderCompletedEvent event) {
        entryTokenRepository.delete(event.userId());
    }
}
