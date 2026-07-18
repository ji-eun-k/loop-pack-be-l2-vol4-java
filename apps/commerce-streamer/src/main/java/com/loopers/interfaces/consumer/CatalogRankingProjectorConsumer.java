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

/**
 * catalog_event_ledger CDC 스트림을 구독해 실시간 랭킹 점수를 갱신하는 프로젝터.
 * Redis 랭킹이 유실되는 경우 RankingRecoveryService가 같은 원장을 다시 읽어 복구한다.
 */
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
        // 배치 안에 여러 날짜의 이벤트가 섞일 수 있어(자정 전후 등) 날짜별로 묶어서
        // RankingUpdater.applyEvents(date, ...)를 날짜당 한 번씩만 호출한다.
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
