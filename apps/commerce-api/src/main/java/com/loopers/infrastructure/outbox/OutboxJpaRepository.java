package com.loopers.infrastructure.outbox;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

import jakarta.persistence.QueryHint;
import java.util.List;

public interface OutboxJpaRepository extends JpaRepository<OutboxEventEntity, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("SELECT o FROM OutboxEvent o WHERE o.status = 'PENDING' OR (o.status = 'FAILED' AND o.retryCount < 5) ORDER BY o.id ASC LIMIT 500")
    List<OutboxEventEntity> findPendingWithSkipLocked();
}
