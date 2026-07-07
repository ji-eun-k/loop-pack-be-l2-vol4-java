package com.loopers.interfaces.api.queue;

import com.loopers.domain.queue.QueueEntryResult;
import com.loopers.domain.queue.QueuePositionResult;

public class QueueV1Dto {

    public record EnterResponse(
            String status,
            long position,
            long waitingCount,
            long estimatedWaitSeconds
    ) {
        public static EnterResponse from(QueueEntryResult result) {
            return new EnterResponse(
                    result.status().name(),
                    result.position(),
                    result.waitingCount(),
                    result.estimatedWaitSeconds()
            );
        }
    }

    public record PositionResponse(
            String status,
            long position,
            long waitingCount,
            long nextPollAfterMs,
            long estimatedWaitSeconds
    ) {
        public static PositionResponse from(QueuePositionResult result) {
            return new PositionResponse(
                    result.status().name(),
                    result.position(),
                    result.waitingCount(),
                    result.nextPollAfterMs(),
                    result.estimatedWaitSeconds()
            );
        }
    }
}
