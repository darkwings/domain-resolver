package com.nttdata.poc.rest;

import java.util.Map;

public interface RequestFactory<A> extends Configurable {

    Request createRequest(A a);

    default void configure(Map<String, ?> map) {
        // Do nothing
    }
}
