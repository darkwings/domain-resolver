package com.nttdata.poc;

import com.google.common.io.Resources;
import com.nttdata.poc.model.Activity;
import com.nttdata.poc.model.ActivityEnriched;
import com.nttdata.poc.model.Domain;
import com.nttdata.poc.serializer.JsonSerDes;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.net.URL;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.kafka.streams.KafkaStreams.State.REBALANCING;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

/**
 * TODO configurare la retention dello state store
 */
@Slf4j
public class DomainResolver {

    static final String STORE_NAME = "domain-store";

    private final Options options;
    private final Monitor monitor;

    public static void main(String[] args) throws Exception {
        val options = Options.loadFrom(args[0]);
        log.info("Configuration report: {}", options);
        val resolver = new DomainResolver(options);
        resolver.start();
    }

    public DomainResolver(Options options) {
        this.options = options;
        this.monitor = Monitor.create();
    }

    void start() {
        val streams = new KafkaStreams(createTopology(), properties(options));
        streams.setGlobalStateRestoreListener(new RestoreListener());
        streams.setUncaughtExceptionHandler(t -> {
            log.error("Caught exception ", t);
            return REPLACE_THREAD;
        });
        streams.setStateListener((newState, oldState) -> {
            log.info("New state '{}', old state was '{}'", newState, oldState);
            if (newState == REBALANCING) monitor.addRebalance();
        });
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    protected Topology createTopology() {

        val builder = new StreamsBuilder();

        val activityBranches = builder
                .stream(options.getSourceTopic(),
                        Consumed.with(Serdes.String(), JsonSerDes.activity())
                                .withTimestampExtractor(new CustomTimestampExtractor()))
                // .peek((k, v)-> System.out.println("=======> RECEIVED " + v))
                .split(Named.as("Activity-"))
                .branch(this::enrichOff, Branched.as("dlq"))
                .branch(this::enrichTriggered, Branched.as("enrich-triggered"))
                .defaultBranch(Branched.as("undefined"));

        // ======================================================================================
        // Se il dominio non è presente nel dato entrante,
        // mando il dato in DLQ
        activityBranches.get("Activity-dlq")
                .peek((k, v) -> monitor.addDlqMessage())
                // .peek((k,v) -> System.out.println("=======>TO DLQ NOW!!"))
                .to(options.getDlqTopic(), Produced.with(Serdes.String(), JsonSerDes.activity()));


        // ======================================================================================
        // Se il dominio è presente, provo a verificare se è sospetto o no con una join
        // con la tabella domains.
        // Devo cambiare chiave, in modo da poter fare una join (co-partitioned) con la tabella
        // dei domini.
        val activities = activityBranches.get("Activity-enrich-triggered")
                .selectKey(this::lookupKey);

        // A questo punto, posso fare una lookup del dominio
        // Configuro il changelog topic in modo da avere una replica già pronta su un'altra istanza
        // utilizzando min.insync.replicas
        val topicConfigs = new HashMap<String, String>();
        topicConfigs.put("min.insync.replicas", Integer.toString(options.getMinInSyncReplicas()));

        val domainTable =
                builder.table(options.getLookupTableTopic(), Consumed.with(Serdes.String(), JsonSerDes.domain()),
                        Materialized.<String, Domain, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
                                .withLoggingEnabled(topicConfigs));

        // Qui c'è l'effettivo lookup con lo State Store
        val activityEnriched = activities
                .leftJoin(domainTable,
                        (activity, domain) -> new ActivityEnriched(activity, domain != null ? domain.getSuspect() : null),
                        Joined.with(Serdes.String(), JsonSerDes.activity(), JsonSerDes.domain()));

        // Controllo enrichment effettuato
        // Se nello stream enriched ho suspect=null, devo fare una query sul sistema esterno
        // e poi scrivere sul topic di lookup per aggiornare lo State Store

        val enrichBranches = activityEnriched
                .split(Named.as("Enrich-"))
                .branch(this::enrichAvailable, Branched.as("done"))
                .branch(this::enrichRequired, Branched.as("to-be-done"))
                .defaultBranch(Branched.as("def"));

        // Caso 1
        // Enrichment presente, posso scrivere direttamente sul topic destinazione,
        // ma devo fare una rekey per riportare la chiave al valore iniziale "activityId"
        enrichBranches.get("Enrich-done")
                .selectKey(this::enrichedStreamKey)
                // .peek((k,v) -> System.out.println("=======> ENRICH_DONE " + v))
                .peek((k, v) -> monitor.addMessageProcessed())
                .to(options.getDestTopic(), Produced.with(Serdes.String(), JsonSerDes.activityEnriched()));

        // Caso 2
        // Informazioni di enrichment non in cache
        // (il dominio non è definito sullo state store) -> interrogo il sistema
        // esterno e poi salvo il dato sul topic che alimenta lo state store

        val enrichNeeded = enrichBranches
                .get("Enrich-to-be-done")
                // .peek((k,v) -> System.out.println("=======> ENRICH_TO_BE_DONE"))
                .transformValues(supplyValueTransformer(options), STORE_NAME);


        val enrichNeededBranches = enrichNeeded
                .split(Named.as("Client-"))
                .branch((k, v) -> v.state == IntermediateResultState.OK, Branched.as("OK"))
                .branch((k, v) -> v.state == IntermediateResultState.KO, Branched.as("KO"))
                .defaultBranch(Branched.as("cd"));


        // 2a:
        // se l'interrogazione al sistema esterno fallisce, il record originale va in DLQ
        enrichNeededBranches.get("Client-KO")
                .mapValues(v -> v.enrichedData.getActivity())
                .selectKey((k, v) -> v.getActivityId())
                // .peek((k,v) -> System.out.println("=======> CLIENT KO - RETRY"))
                .peek((k, v) -> monitor.addRetryMessage()) // TODO Retry logic da raffinare!
                .to(options.getRetryTopic(), Produced.with(Serdes.String(), JsonSerDes.activity()));


        // 2b:
        // interrogazione OK di sistema esterno.
        val branchOk = enrichNeededBranches.get("Client-OK");

        //   2b-1
        //   L'informazione di arricchimento va sullo state store (attraverso il topic dedicato)
        branchOk.mapValues(v -> v.lookup)
                // .peek((k,v) -> System.out.println("=======> TO LOOKUP"))
                .to(options.getLookupTableTopic(), Produced.with(Serdes.String(), JsonSerDes.domain()));

        //   2b-2
        //   Il dato arricchito va sul topic destinazione per i downstream consumer
        branchOk.mapValues(v -> v.enrichedData)
                .selectKey(this::enrichedStreamKey)
                // .peek((k,v) -> System.out.println("=======> TO OUTPUT"))
                .peek((k, v) -> monitor.addMessageProcessed())
                .to(options.getDestTopic(), Produced.with(Serdes.String(), JsonSerDes.activityEnriched()));

        return builder.build();
    }

    protected boolean enrichTriggered(String s, Activity activity) {
        return activity.getDomain() != null && activity.getDomain().trim().length() > 0;
    }

    protected boolean enrichOff(String s, Activity activity) {
        return activity.getDomain() == null || activity.getDomain().trim().length() == 0;
    }

    protected boolean enrichAvailable(String s, ActivityEnriched activityEnriched) {
        return activityEnriched.getSuspect() != null;
    }

    protected boolean enrichRequired(String s, ActivityEnriched activityEnriched) {
        return activityEnriched.getSuspect() == null;
    }

    protected String lookupKey(String s, Activity activity) {
        return activity.getDomain();
    }

    protected String enrichedStreamKey(String s, ActivityEnriched activityEnriched) {
        return activityEnriched.getActivity().getActivityId();
    }

    protected ValueTransformerSupplier<ActivityEnriched, IntermediateResult<ActivityEnriched, Domain>> supplyValueTransformer(Options options) {
        return () -> new ValueTransformer<>() {
            private OkHttpClient client;
            private KeyValueStore<String, ValueAndTimestamp<Domain>> kvStore;
            private Cancellable punctuator;
            private String jsonRaw;

            @SneakyThrows
            @Override
            public void init(ProcessorContext context) {
                client = new OkHttpClient();
                this.kvStore = context.getStateStore(STORE_NAME);
                punctuator = context.schedule(Duration.ofMillis(options.getStateStoreTtlPeriodMs()),
                        PunctuationType.WALL_CLOCK_TIME, this::enforceTtl);
                URL url = Resources.getResource("safebrowsing-payload.json");
                jsonRaw = Resources.toString(url, Charset.defaultCharset());
            }

            @Override
            public IntermediateResult<ActivityEnriched, Domain> transform(ActivityEnriched activityEnriched) {
                log.info("Calling external services for {}", activityEnriched.getActivity().getDomain());

                // =========================================================================
                // FIXME Ovviamente tutta questa parte va buttata e rifatta
                //    per essere il più generale possibile
                try {
                    val json = jsonRaw.replace("%DOMAIN%", activityEnriched.getActivity().getDomain());
                    val body = RequestBody.create(MediaType.parse("application/json"), json);
                    val request = new Request.Builder()
                            .url(options.getEndpointUrl() + System.getProperty("api.key"))
                            .post(body)
                            .build();
                    val call = client.newCall(request);
                    val response = call.execute();
                    val r = response.body().string();
                    // System.out.println("======> " + r);
                    log.info("Received '{}' for domain '{}'", r, activityEnriched.getActivity().getDomain());
                    val suspect = !r.startsWith("{}");
                    activityEnriched.mark(suspect);
                    return new IntermediateResult<>(activityEnriched,
                            new Domain(activityEnriched.getActivity().getDomain(), suspect,
                                    Instant.now().toString()),
                            IntermediateResultState.OK);
                } catch (Exception e) {
                    log.error(String.format("Failed to get domain info for %s",
                            activityEnriched.getActivity().getDomain()), e);
                    return new IntermediateResult<>(activityEnriched,
                            new Domain(activityEnriched.getActivity().getDomain(),
                                    true, Instant.now().toString()),
                            IntermediateResultState.KO);
                }
                // =========================================================================
            }

            private void enforceTtl(Long timestamp) {
                try (KeyValueIterator<String, ValueAndTimestamp<Domain>> iter = kvStore.all()) {
                    while (iter.hasNext()) {
                        KeyValue<String, ValueAndTimestamp<Domain>> entry = iter.next();
                        log.debug("[TTL] Checking to see if record has expired for key '{}'", entry.key);
                        ValueAndTimestamp<Domain> lastDomain = entry.value;
                        if (lastDomain == null) {
                            continue;
                        }

                        Instant lastUpdated = Instant.parse(lastDomain.value().getTimestamp());
                        long millisFromLastUpdate = Duration.between(lastUpdated, Instant.now()).toMillis();
                        if (millisFromLastUpdate >= options.getStateStoreTtlMs()) {
                            log.info("[TTL] Deleting entry for key '{}'", entry.key);
                            kvStore.delete(entry.key);
                        }
                    }
                }
            }

            @Override
            public void close() {
                punctuator.cancel();
            }
        };
    }

    enum IntermediateResultState {
        OK, KO
    }

    @Value
    static class IntermediateResult<A, L> {
        A enrichedData;
        L lookup;
        IntermediateResultState state;
    }

    protected Properties properties(Options options) {
        val p = new Properties();
        p.put(BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers());
        p.put(STATE_DIR_CONFIG, options.getStateStoreDir());
        p.put(APPLICATION_ID_CONFIG, options.getApplicationId());
        p.put(CACHE_MAX_BYTES_BUFFERING_CONFIG, options.getCacheMaxBufferingBytes());
        p.put(COMMIT_INTERVAL_MS_CONFIG, options.getCommitIntervalMs());

        p.put(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        //p.put(DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, AlwaysContinueProductionExceptionHandler.class.getName())
        return p;
    }
}
