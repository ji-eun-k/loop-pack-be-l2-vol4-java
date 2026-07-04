package com.loopers.application.event;

public record UserActionEvent(UserActionType actionType, Long userId, Long resourceId) {
}