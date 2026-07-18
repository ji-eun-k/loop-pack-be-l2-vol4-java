package com.loopers.application.ranking;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Redis 랭킹이 유실(장애/재기동 등)됐을 가능성에 대비해 주기적으로 원장(ledger) 기반 리플레이를 시도한다.
 * 이미 완료됐거나 다른 파드가 처리 중이면 RankingRecoveryService가 스스로 skip하므로
 * 여기서는 매 tick마다 무조건 호출해도 안전하다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "ranking.recovery", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RankingRecoveryScheduler {

    private final RankingRecoveryService recoveryService;

    @Scheduled(
        initialDelayString = "${ranking.recovery.initial-delay:30s}",
        fixedDelayString = "${ranking.recovery.fixed-delay:1m}"
    )
    public void recover() {
        try {
            RankingRecoveryService.RecoveryResult result = recoveryService.recoverTodayIfNecessary();
            if (result.status() == RankingRecoveryService.Status.COMPLETED) {
                log.info("[RANKING_RECOVERY] completed. date={}, scanned={}, applied={}",
                    result.date(), result.scanned(), result.applied());
            }
        } catch (RuntimeException exception) {
            // 실패해도 예외를 삼켜서 스케줄러 자체가 죽지 않게 하고 다음 tick에 재시도한다.
            log.error("[RANKING_RECOVERY] failed; it will retry on the next schedule", exception);
        }
    }
}
