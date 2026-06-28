package com.loopers;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SyncTaskExecutor;

import java.util.concurrent.Executor;

@TestConfiguration
public class TestAsyncConfig {

    @Bean(name = "taskExecutor")
    public Executor taskExecutor() {
        return new SyncTaskExecutor();
    }
}