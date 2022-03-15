package com.nttdata.poc;

import com.nttdata.poc.model.SessionIn;
import com.nttdata.poc.model.SessionData;
import com.nttdata.poc.serializer.JsonSerDes;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Duration;
import java.time.Instant;
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
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SessionWindowSample {

    String topicIn;
    String topicOut;
    String applicationId;
    int neededInput;

    public SessionWindowSample(String topicIn, String topicOut, String applicationId, int neededInput) {
        this.topicIn = topicIn;
        this.topicOut = topicOut;
        this.applicationId = applicationId;
        this.neededInput = neededInput;

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

        KStream<String, SessionIn> in = builder.stream(topicIn,
                Consumed.with(Serdes.String(), JsonSerDes.sessionIn())
                        .withTimestampExtractor(new CustomSessionTimestampExtractor()));

        val rekeyStream = in.selectKey((k, v) -> v.getActivity().getActivityId());

        rekeyStream
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(30), Duration.ofSeconds(5))) // TODO configurabile
                .aggregate(SessionData::new,
                        (key, value, aggregate) -> {
                            if (aggregate.isEmpty()) {
                                aggregate.init(value.getActivity());
                            }
                            aggregate.addLogType(value.getLogType());
                            return aggregate;
                        }, (aggKey, aggOne, aggTwo) -> null)
                .filter((k, v) -> v.isComplete(neededInput)) // Branch, se il messaggio non è completo, va in DLQ
                .toStream()
                .map((windowedKey, data) -> {
                    return KeyValue.pair(windowedKey.key(), data);
                })
                .mapValues(SessionData::getActivity)
                .to(topicOut, Produced.with(Serdes.String(), JsonSerDes.activity()));
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

    /**
     * Esempio di Custom TS extractor
     *
     */
    public static class CustomSessionTimestampExtractor implements TimestampExtractor {

        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            SessionIn a = (SessionIn) record.value();
            if (a != null && a.getActivity() != null && a.getActivity().getTimestamp() != null) {
                return Instant.parse(a.getActivity().getTimestamp()).toEpochMilli();
            }
            return partitionTime;
        }
    }
}
