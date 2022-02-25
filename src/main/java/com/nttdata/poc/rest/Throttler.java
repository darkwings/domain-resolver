package com.nttdata.poc.rest;

/**
 * TBD: rate limiting for external calls, in order not to be blacklisted.
 * We should support a rate of request per second.
 */
public interface Throttler extends Configurable {

    // TODO
    // Guava RateLimiter
    // custom RingBuffer

    double acquire();
}
