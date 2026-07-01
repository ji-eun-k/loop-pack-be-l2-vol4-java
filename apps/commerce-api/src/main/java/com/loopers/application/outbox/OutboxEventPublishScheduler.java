package com.loopers.application.outbox;

import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class OutboxEventPublishScheduler {

    private static final int MAX_RETRY_COUNT = 5;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 1_000)
    @Transactional
    public void publish() {
        List<OutboxEvent> pending = outboxRepository.findPending();
        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate.send(event.getTopicName(), event.getPartitionKey(), event.getPayload()).get();
                event.markPublished();
            } catch (Exception e) {
                event.markFailed(e.getMessage());
                if (event.getRetryCount() >= MAX_RETRY_COUNT) {
                    log.error("[OUTBOX] 최대 재시도 초과 — id={}, eventType={}", event.getId(), event.getEventType());
                }
            }
            outboxRepository.save(event);
        }
    }
}
