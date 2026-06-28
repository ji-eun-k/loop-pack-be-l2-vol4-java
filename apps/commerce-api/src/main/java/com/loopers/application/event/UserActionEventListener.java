package com.loopers.application.event;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class UserActionEventListener {

    @Async
    @EventListener
    public void handle(UserActionEvent event) {
        log.info("[USER_ACTION] action={}, userId={}, resourceId={}", event.actionType(), event.userId(), event.resourceId());
    }
}