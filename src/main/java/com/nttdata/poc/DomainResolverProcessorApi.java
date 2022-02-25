package com.nttdata.poc;

import com.nttdata.poc.model.Activity;
import com.nttdata.poc.model.ActivityEnriched;
import com.nttdata.poc.model.Domain;
import com.nttdata.poc.serializer.JsonSerDes;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

/**
 * @deprecated lo state store non sarebbe partizionato, a meno che il topic sorgente non contenga la stessa key
 * da utilizzare per lo state store
 */
@Slf4j
@Deprecated
public class DomainResolverProcessorApi {

    private static final String JSON_TEMPLATE = "{\n" +
            "  \"client\": {\n" +
            "    \"clientId\": \"yourcompanyname\",\n" +
            "    \"clientVersion\": \"1.5.2\"\n" +
            "  },\n" +
            "  \"threatInfo\": {\n" +
            "    \"threatTypes\": [\n" +
            "      \"MALWARE\",\n" +
            "      \"SOCIAL_ENGINEERING\",\n" +
            "      \"THREAT_TYPE_UNSPECIFIED\"\n" +
            "    ],\n" +
            "    \"platformTypes\": [\n" +
            "      \"ALL_PLATFORMS\"\n" +
            "    ],\n" +
            "    \"threatEntryTypes\": [\n" +
            "      \"URL\"\n" +
            "    ],\n" +
            "    \"threatEntries\": [\n" +
            "      {\n" +
            "        \"url\": \"%DOMAIN%\"\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}";

    private final String bootstrapServers;
    private final String sourceTopic;
    private final String destTopic;
    private final String stateDir;
    private final String apiKey;
    private final long periodTtl;
    private final long domainTtlMillis;

    public DomainResolverProcessorApi(String bootstrapServers, String sourceTopic,
                                      String destTopic, String stateDir, String apiKey,
                                      long periodTtl,
                                      long domainTtlMillis) {
        requireNonNull(bootstrapServers, "bootstrapServers cannot be null");
        requireNonNull(sourceTopic, "sourceTopic cannot be null");
        requireNonNull(destTopic, "destTopic cannot be null");
        requireNonNull(apiKey, "apiKey cannot be null");
        this.bootstrapServers = bootstrapServers;
        this.sourceTopic = sourceTopic;
        this.destTopic = destTopic;
        this.stateDir = stateDir;
        this.apiKey = apiKey;
        this.periodTtl = periodTtl;
        this.domainTtlMillis = domainTtlMillis;
    }

    public static void main(String[] args) {
        String bootstrapServers = args[0];
        String sourceTopic = args[1];
        String destTopic = args[2];
        String stateDir = args[3];
        String apiKey = args[4];
        long periodTtl = Long.parseLong(args[5]);
        long domainTtlMillis = Long.parseLong(args[6]);
        val resolver = new DomainResolverProcessorApi(bootstrapServers, sourceTopic, destTopic,
                stateDir, apiKey, periodTtl, domainTtlMillis);
        resolver.start();
    }


    private void start() {
        val streams = new KafkaStreams(createTopology(), properties());
        streams.setUncaughtExceptionHandler(exception -> REPLACE_THREAD);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.REBALANCING) {
                // Do anything that's necessary to manage rebalance
                log.info("Rebalancing");
            }
        });
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Topology createTopology() {

        Topology builder = new Topology();

        builder.addSource("Activity", Serdes.String().deserializer(),
                JsonSerDes.activity().deserializer(), sourceTopic);

        builder.addProcessor("Activity processor",
                () -> ActivityProcessor.with(periodTtl, domainTtlMillis, apiKey),
                "Activity");

        Map<String, String> topicConfigs = new HashMap<>();
        // topicConfigs.put("min.insync.replicas", "2"); // TODO
        // topicConfigs.put(RETENTION_MS_CONFIG, Long.toString(domainTtlMillis)); // TODO

        // TODO il problema qui è che la chiave dello store non è la partition key, per cui
        //    lo store non è partizionato correttamente, tutte le istanze potrebbero avere gli
        //    stessi dati. Alternativa, fare qualcosa del genere, ma a quel punto meglio DSL e selectKey
        //    topology.addSink(...); write to new topic for repartitioning
        //    topology.addSource(...); // read from repartition topic
        StoreBuilder<KeyValueStore<String, Domain>> storeBuilder =
                Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("domain-store"),
                                Serdes.String(), JsonSerDes.domain())
                        .withLoggingEnabled(topicConfigs);
        builder.addStateStore(storeBuilder, "Activity processor");

        builder.addSink("Activity enriched Sink",
                destTopic, Serdes.String().serializer(), JsonSerDes.activityEnriched().serializer(),
                "Activity processor");

        return builder;
    }

    public static class ActivityProcessor implements Processor<String, Activity, String, ActivityEnriched> {

        private ProcessorContext<String, ActivityEnriched> context;
        private KeyValueStore<String, Domain> kvStore;
        private OkHttpClient client;
        private Cancellable punctuator;
        private final long periodTtl;
        private final long domainTtlMillis;
        private final String apiKey;

        static ActivityProcessor with(long periodTtl, long domainTtlMillis, String apiKey) {
            return new ActivityProcessor(periodTtl, domainTtlMillis, apiKey);
        }

        private ActivityProcessor(long periodTtl, long domainTtlMillis, String apiKey) {
            this.periodTtl = periodTtl;
            this.domainTtlMillis = domainTtlMillis;
            this.apiKey = apiKey;
        }

        @Override
        public void init(ProcessorContext<String, ActivityEnriched> context) {
            this.context = context;
            this.kvStore = context.getStateStore("domain-store");
            client = new OkHttpClient();
            punctuator =
                    this.context.schedule(Duration.ofMillis(periodTtl),
                            PunctuationType.WALL_CLOCK_TIME, this::enforceTtl);
        }

        @Override
        public void process(Record<String, Activity> record) {

            Activity activity = record.value();
            val domainStr = activity.getDomain();
            Domain domain = kvStore.get(domainStr);
            if (domain == null) {
                log.warn("===================================");
                log.warn("Calling external services for {}", activity.getDomain());

                try {
                    val json = JSON_TEMPLATE.replace("%DOMAIN%", activity.getDomain());
                    val body = RequestBody.create(MediaType.parse("application/json"), json);
                    val request = new Request.Builder()
                            .url("https://safebrowsing.googleapis.com/v4/threatMatches:find?key=" + apiKey)
                            .post(body)
                            .build();
                    val call = client.newCall(request);
                    val response = call.execute();
                    val r = response.body().string();
                    log.info("Received '{}' for domain '{}'", r, activity.getDomain());
                    val suspect = !r.startsWith("{}");
                    domain = new Domain(activity.getDomain(), suspect, Instant.now().toString());
                    kvStore.put(activity.getDomain(), domain);
                } catch (Exception e) {
                    log.error("Failed to get domain info for {}", activity.getDomain());
                    log.error("Error is", e);

                    // TODO possibilità di rimettere il dato nel topic di input per un eventuale reprocess

                    // context.forward(null);

                    // TODO DLQ?

                    domain = new Domain(activity.getDomain(), true, Instant.now().toString());
                    kvStore.put(activity.getDomain(), domain);
                }
            }
            val activityEnriched = new ActivityEnriched(activity, domain.getSuspect());
            context.forward(new Record<>(record.key(), activityEnriched, record.timestamp()));
        }

        public void enforceTtl(Long timestamp) {
            try (KeyValueIterator<String, Domain> iter = kvStore.all()) {
                while (iter.hasNext()) {
                    KeyValue<String, Domain> entry = iter.next();
                    log.info("Checking to see if domain record has expired: {}", entry.key);
                    Domain lastDomain = entry.value;
                    if (lastDomain == null) {
                        continue;
                    }

                    Instant lastUpdated = Instant.parse(lastDomain.getTimestamp());
                    long millisFromLastUpdate = Duration.between(lastUpdated, Instant.now()).toMillis();
                    if (millisFromLastUpdate >= domainTtlMillis) {
                        kvStore.delete(entry.key);
                    }
                }
            }
        }


        @Override
        public void close() {
            punctuator.cancel();
        }
    }


    private Properties properties() {
        val properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "domain-resolution-processor-api");
        return properties;
    }
}
