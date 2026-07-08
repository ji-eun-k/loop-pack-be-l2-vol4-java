package com.loopers.interfaces.api.queue;

import com.loopers.application.queue.QueueEntry;
import com.loopers.application.queue.QueuePosition;

public class QueueV1Dto {

    public record EnterResponse(
            String status,
            long position,
            long waitingCount,
            long estimatedWaitSeconds
    ) {
        public static EnterResponse from(QueueEntry entry) {
            return new EnterResponse(
                    entry.status().name(),
                    entry.position(),
                    entry.waitingCount(),
                    entry.estimatedWaitSeconds()
            );
        }
    }

    public record PositionResponse(
            String status,
            long position,
            long waitingCount,
            long nextPollAfterMs,
            long estimatedWaitSeconds,
            String entryToken
    ) {
        public static PositionResponse from(QueuePosition position) {
            return new PositionResponse(
                    position.status().name(),
                    position.position(),
                    position.waitingCount(),
                    position.nextPollAfterMs(),
                    position.estimatedWaitSeconds(),
                    position.entryToken()
            );
        }
    }
}
