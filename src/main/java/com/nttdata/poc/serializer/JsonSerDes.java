package com.nttdata.poc.serializer;


import com.nttdata.poc.model.Activity;
import com.nttdata.poc.model.ActivityEnriched;
import com.nttdata.poc.model.Domain;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerDes {

    public static Serde<Activity> activity() {
        return Serdes.serdeFrom(new JsonSerializer<>(),
                new JsonDeserializer<>(Activity.class));
    }

    public static Serde<ActivityEnriched> activityEnriched() {
        return Serdes.serdeFrom(new JsonSerializer<>(),
                new JsonDeserializer<>(ActivityEnriched.class));
    }

    public static Serde<Domain> domain() {
        return Serdes.serdeFrom(new JsonSerializer<>(),
                new JsonDeserializer<>(Domain.class));
    }
}
