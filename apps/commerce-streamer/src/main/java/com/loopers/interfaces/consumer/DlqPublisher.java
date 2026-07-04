package com.loopers.interfaces.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

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
