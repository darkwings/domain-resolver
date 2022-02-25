package com.nttdata.poc.rest.impl;

import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class GuavaThrottlerTest {

    @Test
    public void checkThrottle() {

        Map<String, Object> conf = Map.of("permitsPerSecond", 5);
        val throttler = new GuavaThrottler();
        throttler.configure(conf);

        throttler.acquire();
        IntStream.range(1, 10).forEach(i -> {
            double t = throttler.acquire();
            assertThat(t).isBetween(0.15, 0.25);
        });
    }
}