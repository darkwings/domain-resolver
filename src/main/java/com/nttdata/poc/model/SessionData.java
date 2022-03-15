package com.nttdata.poc.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

import java.util.HashSet;
import java.util.Set;

@FieldDefaults(level = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
@Getter
@AllArgsConstructor
@Builder
public class SessionData {

    Activity activity;
    Set<String> receivedTypes;

    public SessionData() {
        this.receivedTypes = new HashSet<>();
    }

    public boolean isEmpty() {
        return activity == null;
    }

    public void init(Activity activity) {
        this.activity = activity;
    }

    public void addLogType(String logType) {
        receivedTypes.add(logType);
    }

    public boolean isComplete(int expectedMessages) {
        return receivedTypes.size() == expectedMessages;
    }
}

