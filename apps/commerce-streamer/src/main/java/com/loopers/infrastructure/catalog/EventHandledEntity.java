package com.loopers.infrastructure.catalog;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.ZonedDateTime;

@Getter
@Entity(name = "EventHandled")
@Table(name = "event_handled")
public class EventHandledEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(nullable = false)
    private String topic;

    @Column(name = "handled_at", nullable = false, updatable = false)
    private ZonedDateTime handledAt;

    protected EventHandledEntity() {}

    public EventHandledEntity(String eventId, String topic) {
        this.eventId = eventId;
        this.topic = topic;
    }

    @PrePersist
    private void prePersist() {
        this.handledAt = ZonedDateTime.now();
    }
}
