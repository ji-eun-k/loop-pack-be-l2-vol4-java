package com.loopers.interfaces.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 처리 실패한 메시지를 원본 토픽명 + ".dlq" 토픽으로 그대로 재발행해 격리하는 공용 컴포넌트.
 * 여러 컨슈머(CatalogEventLedgerConsumer, CouponIssueConsumer 등)가 공유한다.
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class DlqPublisher {

    private static final String DLQ_SUFFIX = ".dlq";

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public void sendToDlq(ConsumerRecord<Object, Object> record, Exception cause) {
        String dlqTopic = record.topic() + DLQ_SUFFIX;
        kafkaTemplate.send(dlqTopic, record.key(), record.value());
        log.error("[DLQ] 메시지 격리 — topic={}, key={}", dlqTopic, record.key(), cause);
    }
}
