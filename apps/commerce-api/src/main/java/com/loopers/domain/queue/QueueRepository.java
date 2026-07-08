package com.loopers.domain.queue;

import java.util.List;
import java.util.Optional;

public interface QueueRepository {

    /**
     * userId를 대기열에 추가(재진입 시 score 갱신 → 맨 뒤로 밀림).
     * @return [rank(0-based), totalCount]
     */
    List<Long> enter(Long userId);

    /**
     * 대기열에서 현재 순번을 조회한다. 대기열에 없으면 empty.
     */
    Optional<QueuePositionSnapshot> findPositionSnapshot(Long userId);

    /**
     * 대기열 앞에서 n명을 꺼낸다 (score 오름차순, 즉 먼저 진입한 순서).
     */
    List<Long> popOldest(int n);
}
