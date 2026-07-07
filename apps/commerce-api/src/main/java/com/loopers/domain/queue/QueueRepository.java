package com.loopers.domain.queue;

import java.util.List;

public interface QueueRepository {

    /**
     * userId를 대기열에 추가(재진입 시 score 갱신 → 맨 뒤로 밀림).
     * @return [rank(0-based), totalCount]
     */
    List<Long> enter(Long userId);

    /**
     * 현재 대기 상태를 조회한다.
     * @return [statusCode, rank, total]
     *   statusCode: 0=ACTIVE, 1=WAITING, 2=NOT_IN_QUEUE
     *   rank: 0-based (statusCode=1일 때만 유효, 나머지는 -1)
     *   total: (statusCode=1일 때만 유효, 나머지는 -1)
     */
    List<Long> getPosition(Long userId);
}
