package com.loopers.interfaces.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.ranking.RankingEventDateResolver;
import com.loopers.confg.kafka.KafkaConfig;
import com.loopers.domain.ranking.RankingEventScore;
import com.loopers.domain.ranking.RankingScorePolicy;
import com.loopers.infrastructure.catalog.ProductMetricsJpaRepository;
import com.loopers.infrastructure.ranking.RankingUpdater;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Component
public class CatalogViewConsumer {

    private final ProductMetricsJpaRepository productMetricsJpaRepository;
    private final ObjectMapper objectMapper;
    private final DlqPublisher dlqPublisher;
    private final RankingScorePolicy rankingScorePolicy;
    private final RankingUpdater rankingUpdater;
    private final RankingEventDateResolver dateResolver;

    @Transactional
    @KafkaListener(
        topics = "catalog-view-events-v1",
        groupId = "loopers-catalog-view-consumer",
        containerFactory = KafkaConfig.BATCH_LISTENER
    )
    public void consume(List<ConsumerRecord<Object, Object>> records, Acknowledgment ack) {
        Map<LocalDate, List<RankingEventScore>> eventsByDate = new HashMap<>();
        for (ConsumerRecord<Object, Object> record : records) {
            try {
                Map<String, Object> payload = objectMapper.readValue(
                    record.value().toString(), new TypeReference<>() {}
                );
                Long productId = ((Number) payload.get("productId")).longValue();
                productMetricsJpaRepository.upsertViewCountIncrement(productId);
                String eventId = dateResolver.eventId((String) payload.get("eventId"), record);
                LocalDate eventDate = dateResolver.resolve((String) payload.get("occurredAt"), record);
                eventsByDate.computeIfAbsent(eventDate, ignored -> new ArrayList<>())
                    .add(new RankingEventScore(eventId, Map.of(productId, rankingScorePolicy.viewScore())));
            } catch (Exception e) {
                log.warn("[CATALOG_VIEW] 처리 실패 — offset={}", record.offset(), e);
                dlqPublisher.sendToDlq(record, e);
            }
        }
        eventsByDate.forEach(rankingUpdater::applyEvents);
        ack.acknowledge();
    }
}
