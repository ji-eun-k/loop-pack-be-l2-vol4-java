package com.loopers.interfaces.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.confg.kafka.KafkaConfig;
import com.loopers.infrastructure.catalog.ProductMetricsJpaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Component
public class CatalogViewConsumer {

    private final ProductMetricsJpaRepository productMetricsJpaRepository;
    private final ObjectMapper objectMapper;
    private final DlqPublisher dlqPublisher;

    @Transactional
    @KafkaListener(
        topics = "catalog-view-events-v1",
        groupId = "loopers-catalog-view-consumer",
        containerFactory = KafkaConfig.BATCH_LISTENER
    )
    public void consume(List<ConsumerRecord<Object, Object>> records, Acknowledgment ack) {
        for (ConsumerRecord<Object, Object> record : records) {
            try {
                Map<String, Object> payload = objectMapper.readValue(
                    record.value().toString(), new TypeReference<>() {}
                );
                Long productId = ((Number) payload.get("productId")).longValue();
                productMetricsJpaRepository.upsertViewCountIncrement(productId);
            } catch (Exception e) {
                log.warn("[CATALOG_VIEW] 처리 실패 — offset={}", record.offset(), e);
                dlqPublisher.sendToDlq(record, e);
            }
        }
        ack.acknowledge();
    }
}
