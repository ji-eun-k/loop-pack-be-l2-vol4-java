package com.loopers.support.ranking;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * 랭킹 ZSET 키 생성 유틸.
 * streamer(쓰기)와 commerce-api(읽기)가 동일한 키 포맷을 공유하기 위해 modules/redis에 배치한다.
 */
public final class RankingKeys {

    private static final String DAILY_KEY_PREFIX = "ranking:all:";
    private static final String HANDLED_KEY_PREFIX = "ranking:handled:";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    private RankingKeys() {
    }

    public static String daily(LocalDate date) {
        Objects.requireNonNull(date, "date must not be null");
        return DAILY_KEY_PREFIX + date.format(DATE_FORMATTER);
    }

    public static String handled(LocalDate date) {
        Objects.requireNonNull(date, "date must not be null");
        return HANDLED_KEY_PREFIX + date.format(DATE_FORMATTER);
    }
}
