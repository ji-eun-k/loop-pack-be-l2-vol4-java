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

/**
 * 쿠폰 발급 요청 토픽을 소비해 CouponIssueProcessor로 위임한다.
 * 배치 내 한 건이 실패해도 DLQ로 격리하고 나머지는 계속 처리하며, ack는 배치 끝에서 한 번만 수행한다
 * (개별 record 실패가 배치 전체 재처리를 유발하지 않도록).
 */
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
