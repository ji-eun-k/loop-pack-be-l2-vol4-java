package com.loopers.domain.outbox;

import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.Getter;

import java.time.ZonedDateTime;

@Getter
public class OutboxEvent {

    private Long id;
    private String eventType;
    private String payload;
    private String topicName;
    private String partitionKey;
    private String idempotencyKey;
    private OutboxStatus status;
    private int retryCount;
    private String errorMessage;
    private ZonedDateTime createdAt;
    private ZonedDateTime publishedAt;

    public OutboxEvent(String eventType, String payload, String topicName, String partitionKey, String idempotencyKey) {
        validate(eventType, payload, topicName, partitionKey);
        this.eventType = eventType;
        this.payload = payload;
        this.topicName = topicName;
        this.partitionKey = partitionKey;
        this.idempotencyKey = idempotencyKey;
        this.status = OutboxStatus.PENDING;
        this.retryCount = 0;
    }

    public OutboxEvent(Long id, String eventType, String payload, String topicName, String partitionKey,
                       String idempotencyKey, OutboxStatus status, int retryCount,
                       String errorMessage, ZonedDateTime createdAt, ZonedDateTime publishedAt) {
        this.id = id;
        this.eventType = eventType;
        this.payload = payload;
        this.topicName = topicName;
        this.partitionKey = partitionKey;
        this.idempotencyKey = idempotencyKey;
        this.status = status;
        this.retryCount = retryCount;
        this.errorMessage = errorMessage;
        this.createdAt = createdAt;
        this.publishedAt = publishedAt;
    }

    public void markPublished() {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = ZonedDateTime.now();
    }

    public void markFailed(String errorMessage) {
        this.status = OutboxStatus.FAILED;
        this.retryCount++;
        this.errorMessage = errorMessage;
    }

    private void validate(String eventType, String payload, String topicName, String partitionKey) {
        if (eventType == null || eventType.isBlank()) {
            throw new CoreException(ErrorType.BAD_REQUEST, "이벤트 타입은 필수입니다.");
        }
        if (payload == null || payload.isBlank()) {
            throw new CoreException(ErrorType.BAD_REQUEST, "페이로드는 필수입니다.");
        }
        if (topicName == null || topicName.isBlank()) {
            throw new CoreException(ErrorType.BAD_REQUEST, "토픽명은 필수입니다.");
        }
        if (partitionKey == null || partitionKey.isBlank()) {
            throw new CoreException(ErrorType.BAD_REQUEST, "파티션 키는 필수입니다.");
        }
    }
}
