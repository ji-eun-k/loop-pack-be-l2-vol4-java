package com.loopers.application.ranking;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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
            log.error("[RANKING_RECOVERY] failed; it will retry on the next schedule", exception);
        }
    }
}
