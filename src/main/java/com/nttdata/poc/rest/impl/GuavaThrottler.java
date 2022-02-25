package com.nttdata.poc.rest.impl;

import com.google.common.util.concurrent.RateLimiter;
import com.nttdata.poc.rest.Throttler;

import java.util.Map;
import java.util.Optional;

/**
 * Guava based implementation of {@link Throttler}
 *
 * TODO can we do better?
 */
public class GuavaThrottler implements Throttler {

    private RateLimiter rateLimiter;

    @Override
    public void configure(Map<String, ?> map) {
        Integer permitsPerSecond = Optional.of((Integer)map.get("permitsPerSecond"))
                .orElse(Integer.MAX_VALUE);
        rateLimiter = RateLimiter.create(permitsPerSecond);
    }

    @Override
    public double acquire() {
        return rateLimiter.acquire();
    }
}
