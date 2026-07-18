package com.loopers.application.ranking;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * ranking.recovery.* 설정값. 값이 비어있거나 유효하지 않으면 compact constructor에서 기본값으로 보정한다.
 */
@ConfigurationProperties(prefix = "ranking.recovery")
public record RankingRecoveryProperties(
    int batchSize,
    Duration lockTtl,
    Duration markerTtl
) {
    public RankingRecoveryProperties {
        if (batchSize <= 0) {
            batchSize = 1_000;
        }
        // lockTtl: 리플레이 도중 파드가 죽어도 락이 영구히 남지 않도록 하는 안전장치 TTL
        if (lockTtl == null || lockTtl.isNegative() || lockTtl.isZero()) {
            lockTtl = Duration.ofMinutes(30);
        }
        // markerTtl: 완료 마커를 하루 이상 보관해 재기동 시 중복 리플레이를 막는다.
        if (markerTtl == null || markerTtl.isNegative() || markerTtl.isZero()) {
            markerTtl = Duration.ofDays(2);
        }
    }
}