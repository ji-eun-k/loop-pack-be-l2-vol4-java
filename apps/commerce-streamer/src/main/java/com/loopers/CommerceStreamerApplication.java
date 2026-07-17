package com.loopers;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.TimeZone;

/**
 * commerce-streamer: Kafka 이벤트를 소비해 상품 메트릭·랭킹·쿠폰 발급을 처리하는 컨슈머 애플리케이션.
 * EnableScheduling은 랭킹 리커버리/carry-over 등 주기 작업(@Scheduled)을 위해 필요하다.
 */
@EnableScheduling
@ConfigurationPropertiesScan
@SpringBootApplication
public class CommerceStreamerApplication {
    @PostConstruct
    public void started() {
        // JVM 기본 타임존을 서비스 기준 시간대(Asia/Seoul)로 고정해 날짜 계산 오류를 방지한다.
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Seoul"));
    }

    public static void main(String[] args) {
        SpringApplication.run(CommerceStreamerApplication.class, args);
    }
}


