package com.loopers.infrastructure.queue;

import com.loopers.domain.queue.JitterDelay;
import org.springframework.stereotype.Component;

@Component
public class ThreadSleepJitterDelay implements JitterDelay {

    @Override
    public void delay(long millis) {
        if (millis <= 0L) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
