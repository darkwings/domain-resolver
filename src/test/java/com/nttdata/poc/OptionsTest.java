package com.nttdata.poc;

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import lombok.val;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OptionsTest {

    @Test
    @DisplayName("Load should work as expected")
    public void buildOptions() throws IOException {
        URL url = Resources.getResource("config.properties");
        ByteSource s = Resources.asByteSource(url);
        val p = new Properties();
        p.load(new ByteArrayInputStream(s.openStream().readAllBytes()));

        Options options = Options.buildOptions(p);
        assertThat(options.getBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(options.getApplicationId()).isEqualTo("myapp");
        assertThat(options.getSourceTopic()).isEqualTo("topic_source");
        assertThat(options.getDestTopic()).isEqualTo("topic_enriched");
        assertThat(options.getLookupTableTopic()).isEqualTo("topic_lookup");
        assertThat(options.getEndpointUrl()).isEqualTo("https://fake.url");
        assertThat(options.getMinInSyncReplicas()).isEqualTo(2);
        assertThat(options.isStateStoreTtlEnabled()).isTrue();
        assertThat(options.getStateStoreTtlPeriodMs()).isEqualTo(14500L);
        assertThat(options.getStateStoreTtlMs()).isEqualTo(12000L);
        assertThat(options.getCacheMaxBufferingBytes()).isEqualTo(1111L);
        assertThat(options.getCommitIntervalMs()).isEqualTo(3333L);
    }
}