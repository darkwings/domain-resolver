package com.nttdata.poc.rest;

import java.util.Map;
import java.util.Optional;

public interface RequestAuthenticator extends Configurable {

    Optional<String> getAuthorizationHeader();

    default void configure(Map<String, ?> map) {
        // Do nothing
    }
}
