package com.nttdata.poc;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.FileInputStream;
import java.util.Properties;

@Getter
@FieldDefaults(level = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Slf4j
public class Options {

    public static final Integer DEFAULT_MIN_INSYNC_REPLICAS = 1;

    public static final Boolean DEFAULT_TTL_CKECK_ENABLED = Boolean.TRUE;
    public static final Long DEFAULT_TTL_CKECK_PERIOD_MS = 120_000L;
    public static final Long DEFAULT_TTL_MS = 60_000L;

    public static final Long DEFAULT_CACHE_MAX_BYTES_BUFFERING = 1_048_576L;
    public static final Long DEFAULT_COMMIT_INTERVAL_MS = 5000L;


    String bootstrapServers;

    String applicationId;
    String sourceTopic;
    String destTopic;
    String lookupTableTopic;
    int minInSyncReplicas;

    String stateStoreDir;
    boolean stateStoreTtlEnabled;
    long stateStoreTtlPeriodMs;
    long stateStoreTtlMs;

    long cacheMaxBufferingBytes;
    long commitIntervalMs;

    String dlqTopic;

    String retryTopic;

    String endpointUrl;

    static Options loadFrom(String path) throws Exception {
        log.info("Loading configuration from {}", path);
        val p = new Properties();
        p.load(new FileInputStream(path));
        return buildOptions(p);
    }

    static Options buildOptions(Properties p) {
        return Options.builder()
                .bootstrapServers(p.getProperty("bootstrap.servers"))
                .stateStoreDir(p.getProperty("state.store.dir"))
                .sourceTopic(p.getProperty("source.topic"))
                .destTopic(p.getProperty("dest.topic"))
                .lookupTableTopic(p.getProperty("lookup.table.topic"))
                .applicationId(p.getProperty("application.id"))
                .dlqTopic(p.getProperty("dlq.topic"))
                .retryTopic(p.getProperty("retry.topic"))
                .endpointUrl(p.getProperty("endpoint.url"))
                .minInSyncReplicas(Integer.parseInt((String) p.getOrDefault("min.insync.replicas", DEFAULT_MIN_INSYNC_REPLICAS)))
                .stateStoreTtlEnabled("true".equals(p.getOrDefault("state.store.ttl.check.enabled", DEFAULT_TTL_CKECK_ENABLED)))
                .stateStoreTtlPeriodMs(Long.parseLong((String) p.getOrDefault("state.store.ttl.check.period.ms", DEFAULT_TTL_CKECK_PERIOD_MS)))
                .stateStoreTtlMs(Long.parseLong((String) p.getOrDefault("state.store.ttl.ms", DEFAULT_TTL_MS)))
                .cacheMaxBufferingBytes(Long.parseLong((String) p.getOrDefault("cache.max.bytes.buffering", DEFAULT_CACHE_MAX_BYTES_BUFFERING)))
                .commitIntervalMs(Long.parseLong((String) p.getOrDefault("commit.interval.ms", DEFAULT_COMMIT_INTERVAL_MS)))
                .build();
    }
}
