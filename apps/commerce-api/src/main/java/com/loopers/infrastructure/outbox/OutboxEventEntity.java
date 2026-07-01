package com.loopers.infrastructure.outbox;

import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.ZonedDateTime;

@Getter
@Entity(name = "OutboxEvent")
@Table(name = "outbox_event")
public class OutboxEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "topic_name", nullable = false)
    private String topicName;

    @Column(name = "partition_key", nullable = false)
    private String partitionKey;

    @Column(name = "idempotency_key", unique = true)
    private String idempotencyKey;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "created_at", nullable = false, updatable = false)
    private ZonedDateTime createdAt;

    @Column(name = "published_at")
    private ZonedDateTime publishedAt;

    protected OutboxEventEntity() {}

    public OutboxEventEntity(String eventType, String payload, String topicName, String partitionKey, String idempotencyKey) {
        this.eventType = eventType;
        this.payload = payload;
        this.topicName = topicName;
        this.partitionKey = partitionKey;
        this.idempotencyKey = idempotencyKey;
        this.status = OutboxStatus.PENDING;
        this.retryCount = 0;
    }

    @PrePersist
    private void prePersist() {
        this.createdAt = ZonedDateTime.now();
    }

    public OutboxEvent toDomain() {
        return new OutboxEvent(id, eventType, payload, topicName, partitionKey,
            idempotencyKey, status, retryCount, errorMessage, createdAt, publishedAt);
    }

    public void updateFrom(OutboxEvent domain) {
        this.status = domain.getStatus();
        this.retryCount = domain.getRetryCount();
        this.errorMessage = domain.getErrorMessage();
        this.publishedAt = domain.getPublishedAt();
    }
}
