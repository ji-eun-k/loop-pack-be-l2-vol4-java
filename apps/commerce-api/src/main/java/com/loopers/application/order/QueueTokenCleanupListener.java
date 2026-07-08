package com.loopers.application.order;

import com.loopers.domain.queue.EntryTokenRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class QueueTokenCleanupListener {

    private final EntryTokenRepository entryTokenRepository;

    @EventListener
    public void handle(OrderCompletedEvent event) {
        entryTokenRepository.delete(event.userId());
    }
}
