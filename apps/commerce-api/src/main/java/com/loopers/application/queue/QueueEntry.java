package com.loopers.application.queue;

import com.loopers.domain.queue.QueueStatus;

public record QueueEntry(
        QueueStatus status,
        long position,
        long waitingCount,
        long estimatedWaitSeconds
) {}
