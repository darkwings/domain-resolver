package com.nttdata.poc;

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import com.nttdata.poc.model.Activity;
import com.nttdata.poc.model.ActivityEnriched;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled
class DomainResolverSupportTest {

    private DomainResolver domainResolver;

    @BeforeEach
    void beforeEach() throws IOException {
        URL url = Resources.getResource("config.properties");
        ByteSource s = Resources.asByteSource(url);
        val p = new Properties();
        p.load(new ByteArrayInputStream(s.openStream().readAllBytes()));

        Options options = Options.buildOptions(p);
        domainResolver = new DomainResolver(options);
    }

    @Test
    void lookupKey() {
        Activity activity = Activity.builder()
                .domain("www.try.it")
                .build();
        assertThat(domainResolver.lookupKey("k", activity)).isEqualTo("www.try.it");
    }

    @Test
    void enrichedStreamKey() {
        Activity activity = Activity.builder()
                .activityId("1234")
                .domain("www.try.it")
                .build();
        ActivityEnriched ar = ActivityEnriched.builder()
                .activity(activity)
                .build();
        assertThat(domainResolver.enrichedStreamKey("k", ar)).isEqualTo("1234");
    }

    @Test
    void enrichSwitch() {
        Activity activity = Activity.builder()
                .activityId("1234")
                .domain("www.try.it")
                .build();
        ActivityEnriched nav = ActivityEnriched.builder()
                .activity(activity)
                .build();
        ActivityEnriched av = ActivityEnriched.builder()
                .activity(activity)
                .suspect(true)
                .build();
        assertThat(domainResolver.enrichAvailable("k", av)).isTrue();
        assertThat(domainResolver.enrichAvailable("k", nav)).isFalse();

        assertThat(domainResolver.enrichRequired("k", av)).isFalse();
        assertThat(domainResolver.enrichRequired("k", nav)).isTrue();
    }

    @Test
    void enrichTriggered() {
        // Enrich triggered if domain is populated
        assertThat(domainResolver.enrichTriggered("k", Activity.builder().build())).isFalse();
        assertThat(domainResolver.enrichTriggered("k",
                Activity.builder().domain("  ").build())).isFalse();
        assertThat(domainResolver.enrichTriggered("k",
                Activity.builder().domain("").build())).isFalse();

        assertThat(domainResolver.enrichTriggered("k",
                Activity.builder().domain("www.commit.com").build())).isTrue();
    }

    @Test
    void enrichOff() {
        // Enrich is off when enrich target is empty
        assertThat(domainResolver.enrichOff("k", Activity.builder().build())).isTrue();
        assertThat(domainResolver.enrichOff("k",
                Activity.builder().domain("  ").build())).isTrue();
        assertThat(domainResolver.enrichOff("k",
                Activity.builder().domain("").build())).isTrue();

        assertThat(domainResolver.enrichOff("k",
                Activity.builder().domain("www.commit.com").build())).isFalse();
    }
}