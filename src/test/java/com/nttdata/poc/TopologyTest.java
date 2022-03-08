package com.nttdata.poc;

import com.nttdata.poc.model.Activity;
import com.nttdata.poc.model.ActivityEnriched;
import com.nttdata.poc.model.Domain;
import com.nttdata.poc.serializer.JsonSerDes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.junit.jupiter.MockServerSettings;

import java.io.File;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static com.nttdata.poc.DomainResolver.STORE_NAME;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * TODO completare i test di tutti i casi, questo è solo un esempio
 */
@ExtendWith(MockServerExtension.class)
@MockServerSettings(ports = {8787})
class TopologyTest {

    private static final String API_KEY = "aaaaa";

    private MockServerClient client;
    private Properties config;
    private Options options;
    private DomainResolver domainResolver;

    @TempDir
    private File tempDir;

    @BeforeEach
    void beforeEach(MockServerClient client) {
        this.client = client;
        this.client.reset();

        System.setProperty("disable.jmx", "true");

        // FIXME, questo è hardcoded per la prima implementazione con safebrowsing
        System.setProperty("api.key", API_KEY);

        String stateStoreDir = tempDir.getAbsolutePath() + File.separator + UUID.randomUUID();

        config = new Properties();
        config.put(APPLICATION_ID_CONFIG, "test");
        config.put(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(STATE_DIR_CONFIG, stateStoreDir);

        options = Options.builder()
                .bootstrapServers("dummy:9092")
                .stateStoreTtlEnabled(true)
                .lookupTableTopic("test_domains")
                .destTopic("test_enriched")
                .dlqTopic("test_dlq")
                .retryTopic("test_retry")
                .sourceTopic("test_source")
                .applicationId("my_app")
                .cacheMaxBufferingBytes(0)
                .commitIntervalMs(5000)
                .dlqTopic("test_dlq")
                .endpointUrl("http://localhost:8787/endpoint?key=")
                .minInSyncReplicas(1)
                .stateStoreDir(stateStoreDir)
                .stateStoreTtlPeriodMs(120000)
                .stateStoreTtlMs(60000)
                .build();
        domainResolver = new DomainResolver(options);
    }

    @Test
    @DisplayName("Activity without domain should go to DLQ")
    void toDlq() {
        TopologyTestDriver testDriver = new TopologyTestDriver(domainResolver.createTopology(), config);
        TestInputTopic<String, Activity> inputTopic = testDriver.createInputTopic(
                options.getSourceTopic(), new StringSerializer(), JsonSerDes.activity().serializer());
        TestOutputTopic<String, Activity> outputTopic = testDriver.createOutputTopic(
                options.getDlqTopic(),
                new StringDeserializer(), JsonSerDes.activity().deserializer());

        Activity activity = Activity.builder()
                .activityId("key")
                .userId("13123123")
                .ip("120.3.3.1")
                .email("aaa@bbb.it")
                .timestamp(Instant.now().toString())
                .build();
        inputTopic.pipeInput("key", activity);


        // ==============================================================================================
        // Assertions

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key", activity));
    }

    @Test
    @DisplayName("Enriching data with state store content (using cached data, with zero interaction with the remote client)")
    void enrichWithSuspect() {
        TopologyTestDriver testDriver = new TopologyTestDriver(domainResolver.createTopology(), config);
        KeyValueStore<String, ValueAndTimestamp<Domain>> store = testDriver.getTimestampedKeyValueStore(STORE_NAME);
        store.put("www.suspect.it", ValueAndTimestamp.make(Domain.builder()
                        .suspect(true)
                        .domain("www.suspect.it")
                        .timestamp(Instant.now().toString())
                .build(), Instant.now().toEpochMilli()));

        TestInputTopic<String, Activity> inputTopic = testDriver.createInputTopic(
                options.getSourceTopic(), new StringSerializer(), JsonSerDes.activity().serializer());
        TestOutputTopic<String, ActivityEnriched> outputTopic = testDriver.createOutputTopic(
                options.getDestTopic(),
                new StringDeserializer(), JsonSerDes.activityEnriched().deserializer());

        Activity activity = Activity.builder()
                .activityId("12312141")
                .userId("13123123")
                .domain("www.suspect.it")
                .ip("120.3.3.1")
                .email("aaa@bbb.it")
                .timestamp(Instant.now().toString())
                .build();
        inputTopic.pipeInput("12312141", activity);


        // ==============================================================================================
        // Assertions

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("12312141", ActivityEnriched.builder()
                .activity(activity)
                .suspect(true)
                .build()));

        client.verifyZeroInteractions();
    }

    @Test
    @DisplayName("Topology work as expected")
    void happyPath() {
        TopologyTestDriver testDriver = new TopologyTestDriver(domainResolver.createTopology(), config);
        TestInputTopic<String, Activity> inputTopic = testDriver.createInputTopic(
                options.getSourceTopic(), new StringSerializer(), JsonSerDes.activity().serializer());
        TestOutputTopic<String, ActivityEnriched> outputTopic = testDriver.createOutputTopic(
                options.getDestTopic(),
                new StringDeserializer(), JsonSerDes.activityEnriched().deserializer());
        client.when(request()
                        .withMethod("POST")
                        .withPath("/endpoint")
                        .withQueryStringParameter("key", API_KEY)
                )
                .respond(
                        response()
                                .withStatusCode(200)
                                .withBody("{}")
                );

        Activity activity = Activity.builder()
                .activityId("key")
                .userId("13123123")
                .domain("www.test-b.it")
                .ip("120.3.3.1")
                .email("aaa@bbb.it")
                .timestamp(Instant.now().toString())
                .build();
        inputTopic.pipeInput("key", activity);


        // ==============================================================================================
        // Assertions

        client.verify(
                request()
                        .withPath("/endpoint")
                        .withQueryStringParameter("key", API_KEY)
        );


        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key", ActivityEnriched.builder()
                .activity(activity)
                .suspect(false)
                .build()));


        KeyValueStore<String, Domain> store = testDriver.getKeyValueStore(STORE_NAME);
        Domain actual = store.get("www.test-b.it");
        assertThat(actual.getSuspect()).isFalse();
        assertThat(actual.getDomain()).isEqualTo("www.test-b.it");
        assertThat(actual.getTimestamp()).isNotNull();
    }
}
