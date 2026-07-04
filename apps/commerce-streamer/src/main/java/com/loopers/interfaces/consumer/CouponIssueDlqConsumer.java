package com.loopers.interfaces.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.confg.kafka.KafkaConfig;
import com.loopers.domain.coupon.CouponIssueResult;
import com.loopers.infrastructure.coupon.CouponIssueEventJpaRepository;
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
public class CouponIssueDlqConsumer {

    private final CouponIssueEventJpaRepository couponIssueEventJpaRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    @KafkaListener(
        topics = "coupon-issue-requests.dlq",
        groupId = "loopers-coupon-issue-dlq-consumer",
        containerFactory = KafkaConfig.BATCH_LISTENER
    )
    public void consume(List<ConsumerRecord<Object, Object>> records, Acknowledgment ack) {
        for (ConsumerRecord<Object, Object> record : records) {
            try {
                Map<String, Object> data = objectMapper.readValue(record.value().toString(), new TypeReference<>() {});
                String eventId = (String) data.get("eventId");
                couponIssueEventJpaRepository.findByEventId(eventId).ifPresent(entity -> {
                    entity.updateResult(CouponIssueResult.FAILED);
                    couponIssueEventJpaRepository.save(entity);
                });
                log.warn("[COUPON_DLQ] 발급 실패 처리 — eventId={}", eventId);
            } catch (Exception e) {
                log.error("[COUPON_DLQ] DLQ 처리 실패 — offset={}", record.offset(), e);
            }
        }
        ack.acknowledge();
    }
}