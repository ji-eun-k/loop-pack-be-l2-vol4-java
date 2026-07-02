package com.loopers.interfaces.consumer;

import com.loopers.application.catalog.CatalogMetricsProcessor;
import com.loopers.confg.kafka.KafkaConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class CatalogMetricsConsumer {

    private final CatalogMetricsProcessor processor;
    private final DlqPublisher dlqPublisher;

    @KafkaListener(
        topics = "catalog-events-v1",
        groupId = "loopers-catalog-metrics-consumer",
        containerFactory = KafkaConfig.BATCH_LISTENER
    )
    public void consume(List<ConsumerRecord<Object, Object>> records, Acknowledgment ack) {
        for (ConsumerRecord<Object, Object> record : records) {
            String eventType = extractHeader(record, "X-Event-Type");
            String eventId = extractHeader(record, "X-Event-Id");
            String occurredAt = extractHeader(record, "X-Event-Occurred-At");
            String payload = record.value().toString();
            log.debug("[CATALOG] 이벤트 수신 — eventType={}, eventId={}, occurredAt={}", eventType, eventId, occurredAt);
            try {
                processor.process(eventType, eventId, record.topic(), payload);
            } catch (Exception e) {
                dlqPublisher.sendToDlq(record, e);
            }
        }
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
