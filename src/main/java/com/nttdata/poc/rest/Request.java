package com.nttdata.poc.rest;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

@FieldDefaults(level = AccessLevel.PRIVATE)
@EqualsAndHashCode
@ToString
@AllArgsConstructor
@Builder
public class Request {

    public enum Method {
        POST, PUT, GET, DELETE
    }

    @Default
    Method method = Method.GET;

    String url;

    byte[] body;

    @Default
    Map<String, List<String>> queryParams = emptyMap();

    @Default
    Map<String, List<String>> headers = emptyMap();
}
