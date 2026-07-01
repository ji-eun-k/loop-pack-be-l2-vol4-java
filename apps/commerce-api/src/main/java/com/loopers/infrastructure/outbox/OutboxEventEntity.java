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

    @Column(name = "event_id", nullable = false, unique = true, updatable = false)
    private String eventId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "topic_name", nullable = false)
    private String topicName;

    @Column(name = "partition_key", nullable = false)
    private String partitionKey;

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

    public OutboxEventEntity(String eventId, String eventType, String payload, String topicName, String partitionKey) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.payload = payload;
        this.topicName = topicName;
        this.partitionKey = partitionKey;
        this.status = OutboxStatus.PENDING;
        this.retryCount = 0;
    }

    @PrePersist
    private void prePersist() {
        this.createdAt = ZonedDateTime.now();
    }

    public OutboxEvent toDomain() {
        return new OutboxEvent(id, eventId, eventType, payload, topicName, partitionKey,
            status, retryCount, errorMessage, createdAt, publishedAt);
    }

    public void updateFrom(OutboxEvent domain) {
        this.status = domain.getStatus();
        this.retryCount = domain.getRetryCount();
        this.errorMessage = domain.getErrorMessage();
        this.publishedAt = domain.getPublishedAt();
    }
}
