package com.nttdata.poc;

import com.nttdata.poc.serializer.JsonSerDes;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
public class GenericTopology {

    String topicIn;
    String topicOut;
    String enrichField;
    String enrichValue;
    String applicationId;

    GenericTopology(String topicIn, String topicOut, String enrichField, String enrichValue, String applicationId) {
        this.topicIn =topicIn;
        this.topicOut = topicOut;
        this.enrichField = enrichField;
        this.enrichValue = enrichValue;
        this.applicationId = applicationId;

        val streams = new KafkaStreams(createTopology(), properties());
        streams.setGlobalStateRestoreListener(new RestoreListener());
        streams.setUncaughtExceptionHandler(t -> {
            log.error("Caught exception ", t);
            return REPLACE_THREAD;
        });
        streams.setStateListener((newState, oldState) -> {
            log.info("New state '{}', old state was '{}'", newState, oldState);
        });
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Topology createTopology() {
        val builder = new StreamsBuilder();

        KStream<String, Map> in = builder.stream(topicIn, Consumed.with(Serdes.String(), JsonSerDes.map()));

        val stringObjectKStream = in.mapValues(v -> {
            v.put(enrichField, enrichValue);
            return v;
        });
        stringObjectKStream
                .to(topicOut, Produced.with(Serdes.String(), JsonSerDes.map()));
        return builder.build();
    }

    private Properties properties() {
        val p = new Properties();
        p.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(STATE_DIR_CONFIG, "/tmp/" + applicationId + "-" + UUID.randomUUID());
        p.put(APPLICATION_ID_CONFIG, applicationId);
        p.put(CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); // No cache
        p.put(COMMIT_INTERVAL_MS_CONFIG, "5");
        p.put(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        //p.put(DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, AlwaysContinueProductionExceptionHandler.class.getName())
        return p;
    }
}
