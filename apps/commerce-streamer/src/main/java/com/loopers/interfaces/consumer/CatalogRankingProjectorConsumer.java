package com.loopers.interfaces.consumer;

import com.loopers.application.ranking.CatalogRankingScoreMapper;
import com.loopers.domain.catalog.CatalogLedgerEvent;
import com.loopers.confg.kafka.KafkaConfig;
import com.loopers.domain.ranking.RankingEventScore;
import com.loopers.infrastructure.ranking.RankingUpdater;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class CatalogRankingProjectorConsumer {

    private final CatalogLedgerCdcParser cdcParser;
    private final CatalogRankingScoreMapper scoreMapper;
    private final RankingUpdater rankingUpdater;
    private final DlqPublisher dlqPublisher;
    private final Clock clock;

    @KafkaListener(
        topics = "catalog-event-ledger-v1",
        groupId = "loopers-ranking-projector",
        containerFactory = KafkaConfig.BATCH_LISTENER
    )
    public void consume(List<ConsumerRecord<Object, Object>> records, Acknowledgment acknowledgment) {
        Map<LocalDate, List<RankingEventScore>> eventsByDate = new HashMap<>();
        for (ConsumerRecord<Object, Object> record : records) {
            try {
                CatalogLedgerEvent event = cdcParser.parse(record.value());
                Map<Long, Double> deltas = scoreMapper.map(event.eventType(), event.payload());
                if (!deltas.isEmpty()) {
                    LocalDate date = event.occurredAt().atZone(clock.getZone()).toLocalDate();
                    eventsByDate.computeIfAbsent(date, ignored -> new ArrayList<>())
                        .add(new RankingEventScore(event.eventId(), deltas));
                }
            } catch (Exception exception) {
                log.warn("[RANKING_PROJECTOR] 이벤트 변환 실패 — partition={}, offset={}",
                    record.partition(), record.offset(), exception);
                dlqPublisher.sendToDlq(record, exception);
            }
        }
        eventsByDate.forEach(rankingUpdater::applyEvents);
        acknowledgment.acknowledge();
    }
}
