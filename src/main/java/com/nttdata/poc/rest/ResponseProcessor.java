package com.nttdata.poc.rest;

import com.squareup.okhttp.Response;

import java.util.Map;

public interface ResponseProcessor<T> extends Configurable {

    T process(Response response);

    default void configure(Map<String, ?> map) {
        // Do nothing
    }
}
