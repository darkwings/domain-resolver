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
import org.junit.jupiter.api.BeforeEach;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * TODO completare i test, questo è solo un esempio
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

        System.setProperty("disable.jmx", "true");

        // FIXME, questo è hardcoded per la prima implementazione con safebrowsing
        System.setProperty("api.key", API_KEY);

        String stateStoreDir = tempDir.getAbsolutePath() + File.separator + UUID.randomUUID();

        config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

        options = Options.builder()
                .bootstrapServers("dummy:9092")
                .stateStoreTtlEnabled(true)
                .lookupTableTopic("test_domains")
                .destTopic("test_enriched")
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
    @DisplayName("activity without domain should go to DLQ")
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


        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key", activity));
    }

    @Test
    @DisplayName("topology work as expected")
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
