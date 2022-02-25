package com.nttdata.poc.rest.impl;

import com.nttdata.poc.rest.Throttler;

import java.util.Map;

public class RingBufferThrottler implements Throttler {

    @Override
    public void configure(Map<String, ?> var1) {

    }

    @Override
    public double acquire() {
        return 0;
    }
}
