package com.loopers.interfaces.consumer;

import com.loopers.application.catalog.CatalogMetricsProcessor;
import com.loopers.application.ranking.RankingEventDateResolver;
import com.loopers.confg.kafka.KafkaConfig;
import com.loopers.domain.ranking.RankingEventScore;
import com.loopers.infrastructure.ranking.RankingUpdater;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Component
public class CatalogMetricsConsumer {

    private final CatalogMetricsProcessor processor;
    private final DlqPublisher dlqPublisher;
    private final RankingUpdater rankingUpdater;
    private final RankingEventDateResolver dateResolver;

    @KafkaListener(
        topics = "catalog-events-v1",
        groupId = "loopers-catalog-metrics-consumer",
        containerFactory = KafkaConfig.BATCH_LISTENER
    )
    public void consume(List<ConsumerRecord<Object, Object>> records, Acknowledgment ack) {
        Map<LocalDate, List<RankingEventScore>> eventsByDate = new HashMap<>();
        for (ConsumerRecord<Object, Object> record : records) {
            String eventType = extractHeader(record, "X-Event-Type");
            String eventId = extractHeader(record, "X-Event-Id");
            String occurredAt = extractHeader(record, "X-Event-Occurred-At");
            String payload = record.value().toString();
            log.debug("[CATALOG] 이벤트 수신 — eventType={}, eventId={}, occurredAt={}", eventType, eventId, occurredAt);
            try {
                String resolvedEventId = dateResolver.eventId(eventId, record);
                Map<Long, Double> deltas = processor.process(eventType, resolvedEventId, record.topic(), payload);
                if (!deltas.isEmpty()) {
                    LocalDate eventDate = dateResolver.resolve(occurredAt, record);
                    eventsByDate.computeIfAbsent(eventDate, ignored -> new ArrayList<>())
                        .add(new RankingEventScore(resolvedEventId, deltas));
                }
            } catch (Exception e) {
                dlqPublisher.sendToDlq(record, e);
            }
        }
        eventsByDate.forEach(rankingUpdater::applyEvents);
        ack.acknowledge();
    }

    private String extractHeader(ConsumerRecord<Object, Object> record, String key) {
        Header header = record.headers().lastHeader(key);
        if (header == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
