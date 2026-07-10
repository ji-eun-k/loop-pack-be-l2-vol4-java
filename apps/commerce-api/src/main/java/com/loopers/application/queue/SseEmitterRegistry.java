package com.loopers.application.queue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * userId별 SSE emitter를 관리하는 인메모리 레지스트리.
 *
 * 컨트롤러가 emitter를 등록하고, 스케줄러가 매 틱마다 순번 PUSH 및 토큰 발급 시 ACTIVE PUSH를 호출한다.
 * ConcurrentHashMap을 사용하므로 멀티스레드 접근에 안전하다.
 *
 * 단일 인스턴스 가정: 멀티 인스턴스 환경에서는 같은 유저의 emitter가
 * 다른 인스턴스에 등록되어 있을 수 있으므로 Redis Pub/Sub이 필요하다.
 */
@Slf4j
@Component
public class SseEmitterRegistry {

    private final ConcurrentHashMap<Long, SseEmitter> emitters = new ConcurrentHashMap<>();
    // userId별 다음 PUSH 허용 시각(ms). 이 시각 이전에 호출되면 스킵한다.
    private final ConcurrentHashMap<Long, AtomicLong> nextPushAtMs = new ConcurrentHashMap<>();

    /**
     * emitter를 등록하고 종료(완료/타임아웃/에러) 시 자동으로 제거되도록 콜백을 설정한다.
     * remove(key, value)로 같은 객체일 때만 제거해 재연결 시 덮어쓴 emitter가 지워지지 않도록 한다.
     */
    public void register(Long userId, SseEmitter emitter) {
        emitters.put(userId, emitter);
        nextPushAtMs.put(userId, new AtomicLong(0L)); // 연결 즉시 첫 PUSH는 항상 허용
        emitter.onCompletion(() -> cleanup(userId, emitter));
        emitter.onTimeout(() -> cleanup(userId, emitter));
        emitter.onError(e -> cleanup(userId, emitter));
    }

    private void cleanup(Long userId, SseEmitter emitter) {
        // 재연결로 이미 새 emitter로 교체된 경우, 새 연결의 PUSH 스로틀 상태를 지우면 안 된다
        if (emitters.remove(userId, emitter)) {
            nextPushAtMs.remove(userId);
        }
    }

    /**
     * 현재 SSE 연결 중인 userId 목록의 스냅샷을 반환한다.
     * 스케줄러가 순번 PUSH 대상을 순회할 때 사용한다.
     */
    public Set<Long> getRegisteredUserIds() {
        return Set.copyOf(emitters.keySet());
    }

    /**
     * 스케줄러 매 틱마다 호출되며, nextPollAfterMs 기반으로 PUSH 여부를 결정한다.
     * position이 멀수록 PUSH 빈도를 줄여 불필요한 Redis 조회와 네트워크 비용을 낮춘다.
     *
     * position ≤ 50   → 1,000ms마다 (매 틱)
     * position ≤ 500  → 3,000ms마다 (3틱에 1번)
     * position ≤ 5000 → 5,000ms마다 (5틱에 1번)
     * position 초과   → 10,000ms마다 (10틱에 1번)
     */
    public void sendPosition(Long userId, QueuePosition position) {
        SseEmitter emitter = emitters.get(userId);
        if (emitter == null) {
            return;
        }

        long now = System.currentTimeMillis();
        AtomicLong scheduled = nextPushAtMs.get(userId);
        // 아직 push 시각이 되지 않았으면 스킵
        if (scheduled == null || now < scheduled.get()) {
            return;
        }

        try {
            emitter.send(SseEmitter.event()
                    .name("position")
                    .data(position));
            // 다음 PUSH 시각 = 지금 + QueueService가 계산한 nextPollAfterMs
            scheduled.set(now + position.nextPollAfterMs());
        } catch (IOException e) {
            log.debug("SSE position send failed for userId={}", userId, e);
            cleanup(userId, emitter);
        }
    }

    /**
     * 스케줄러가 입장 토큰 발급 직후 호출.
     * 연결 중인 emitter가 없으면(클라이언트가 폴링 방식 사용 또는 연결 끊김) false를 반환한다.
     */
    public boolean sendActive(Long userId, String entryToken) {
        SseEmitter emitter = emitters.get(userId);
        if (emitter == null) {
            return false;
        }
        try {
            emitter.send(SseEmitter.event()
                    .name("active")
                    .data(new ActiveEvent("ACTIVE", entryToken)));
            // 입장 토큰을 받았으므로 더 이상 PUSH가 필요 없어 연결을 닫는다
            emitter.complete();
            return true;
        } catch (IOException e) {
            // 클라이언트가 이미 연결을 끊은 경우 정상 흐름
            log.debug("SSE active send failed for userId={}", userId, e);
            emitters.remove(userId, emitter);
            return false;
        }
    }

    public record ActiveEvent(String status, String entryToken) {}
}
