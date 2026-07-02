package com.loopers.interfaces.consumer;

import com.loopers.application.coupon.CouponIssueProcessor;
import com.loopers.confg.kafka.KafkaConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class CouponIssueConsumer {

    private final CouponIssueProcessor processor;
    private final DlqPublisher dlqPublisher;

    @KafkaListener(
        topics = "coupon-issue-requests",
        groupId = "loopers-coupon-issue-consumer",
        containerFactory = KafkaConfig.BATCH_LISTENER
    )
    public void consume(List<ConsumerRecord<Object, Object>> records, Acknowledgment ack) {
        for (ConsumerRecord<Object, Object> record : records) {
            try {
                processor.process(record.value().toString());
            } catch (Exception e) {
                log.warn("[COUPON_ISSUE] 처리 실패 — offset={}", record.offset(), e);
                dlqPublisher.sendToDlq(record, e);
            }
        }
        ack.acknowledge();
    }
}
