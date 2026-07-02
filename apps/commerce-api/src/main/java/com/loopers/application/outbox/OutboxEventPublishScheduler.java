package com.loopers.application.outbox;

import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class OutboxEventPublishScheduler {

    static final int MAX_RETRY_COUNT = 5;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    @Scheduled(fixedDelay = 1_000)
    @Transactional
    public void publish() {
        List<OutboxEvent> pending = outboxRepository.findPending();
        for (OutboxEvent event : pending) {
            try {
                ProducerRecord<Object, Object> record = new ProducerRecord<>(
                    event.getTopicName(), null, event.getPartitionKey(), event.getPayload()
                );
                record.headers().add("X-Event-Type", event.getEventType().getBytes(StandardCharsets.UTF_8));
                record.headers().add("X-Event-Id", event.getEventId().getBytes(StandardCharsets.UTF_8));
                record.headers().add("X-Event-Occurred-At", event.getCreatedAt().toString().getBytes(StandardCharsets.UTF_8));
                kafkaTemplate.send(record).get();
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
