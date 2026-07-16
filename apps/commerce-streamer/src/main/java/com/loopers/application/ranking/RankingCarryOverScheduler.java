package com.loopers.application.ranking;

import com.loopers.infrastructure.ranking.RankingUpdater;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 매일 23:50에 오늘 랭킹의 0.1배로 내일 키를 미리 생성한다.
 * 자정에 키가 전환되는 순간 랭킹이 텅 비는 콜드 스타트를 방지한다.
 * 자정(00:00)이 아닌 23:50에 도는 이유: 자정에는 이미 새 키가 조회 대상이라 빈 랭킹이 노출되는 공백이 생긴다.
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class RankingCarryOverScheduler {

    private final RankingUpdater rankingUpdater;

    @Scheduled(cron = "0 50 23 * * *", zone = "Asia/Seoul")
    public void carryOver() {
        try {
            long carried = rankingUpdater.carryOverToNextDay();
            if (carried > 0) {
                log.info("[RANKING] 내일 랭킹 전체 carry-over 완료 — count={}", carried);
            } else if (carried == -1) {
                log.info("[RANKING] 다른 파드가 carry-over 실행 중이므로 skip");
            } else if (carried == -2) {
                log.info("[RANKING] 내일 랭킹 키가 이미 존재하므로 carry-over skip");
            } else {
                log.info("[RANKING] 오늘 랭킹이 비어 있어 carry-over skip");
            }
        } catch (Exception e) {
            // 실패해도 다음 날 랭킹이 빈 상태로 시작할 뿐, 이벤트 적립은 정상 동작한다.
            log.error("[RANKING] 내일 랭킹 키 carry-over 실패", e);
        }
    }
}
