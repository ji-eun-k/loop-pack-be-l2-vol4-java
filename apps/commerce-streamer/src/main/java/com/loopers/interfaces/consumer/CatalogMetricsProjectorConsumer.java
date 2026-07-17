package com.loopers.interfaces.consumer;

import com.loopers.application.catalog.CatalogMetricsProcessor;
import com.loopers.domain.catalog.CatalogLedgerEvent;
import com.loopers.confg.kafka.KafkaConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * catalog_event_ledger CDC 스트림을 구독해 product_metrics를 갱신하는 프로젝터.
 * 랭킹 반영(CatalogRankingProjectorConsumer)과 같은 원장 토픽을 각자 다른 groupId로 구독해
 * 서로 독립적으로 동작한다 — 한쪽이 지연/실패해도 다른 쪽 처리에는 영향이 없다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CatalogMetricsProjectorConsumer {

    private final CatalogLedgerCdcParser cdcParser;
    private final CatalogMetricsProcessor metricsProcessor;
    private final DlqPublisher dlqPublisher;

    @KafkaListener(
        topics = "catalog-event-ledger-v1",
        groupId = "loopers-product-metrics-projector",
        containerFactory = KafkaConfig.BATCH_LISTENER
    )
    public void consume(List<ConsumerRecord<Object, Object>> records, Acknowledgment acknowledgment) {
        for (ConsumerRecord<Object, Object> record : records) {
            try {
                CatalogLedgerEvent event = cdcParser.parse(record.value());
                metricsProcessor.process(
                    event.eventType(), event.eventId(), "catalog-event-ledger-v1", event.payload()
                );
            } catch (Exception exception) {
                log.warn("[PRODUCT_METRICS_PROJECTOR] 처리 실패 — partition={}, offset={}",
                    record.partition(), record.offset(), exception);
                dlqPublisher.sendToDlq(record, exception);
            }
        }
        acknowledgment.acknowledge();
    }
}
