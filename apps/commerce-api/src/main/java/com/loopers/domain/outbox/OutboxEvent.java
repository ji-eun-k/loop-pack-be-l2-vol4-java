package com.loopers.domain.outbox;

import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.Getter;

import java.time.ZonedDateTime;
import java.util.UUID;

@Getter
public class OutboxEvent {

    private Long id;
    private String eventId;
    private String eventType;
    private String payload;
    private String topicName;
    private String partitionKey;
    private OutboxStatus status;
    private int retryCount;
    private String errorMessage;
    private ZonedDateTime createdAt;
    private ZonedDateTime publishedAt;

    public OutboxEvent(String eventType, String payload, String topicName, String partitionKey) {
        validate(eventType, payload, topicName, partitionKey);
        this.eventId = UUID.randomUUID().toString();
        this.eventType = eventType;
        this.payload = payload;
        this.topicName = topicName;
        this.partitionKey = partitionKey;
        this.status = OutboxStatus.PENDING;
        this.retryCount = 0;
    }

    public OutboxEvent(Long id, String eventId, String eventType, String payload, String topicName, String partitionKey,
                       OutboxStatus status, int retryCount,
                       String errorMessage, ZonedDateTime createdAt, ZonedDateTime publishedAt) {
        this.id = id;
        this.eventId = eventId;
        this.eventType = eventType;
        this.payload = payload;
        this.topicName = topicName;
        this.partitionKey = partitionKey;
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
