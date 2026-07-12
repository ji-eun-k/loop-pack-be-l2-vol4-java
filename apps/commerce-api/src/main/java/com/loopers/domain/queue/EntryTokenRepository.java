package com.loopers.domain.queue;

import java.util.Optional;

public interface EntryTokenRepository {

    void save(Long userId, String token, long ttlSeconds);

    Optional<String> find(Long userId);

    void delete(Long userId);
}
