package com.nttdata.poc.serializer;


import com.nttdata.poc.model.Activity;
import com.nttdata.poc.model.ActivityEnriched;
import com.nttdata.poc.model.Domain;
import com.nttdata.poc.model.SessionIn;
import com.nttdata.poc.model.SessionData;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;

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

    public static Serde<Map> map() {
        return Serdes.serdeFrom(new JsonSerializer<>(),
                new JsonDeserializer<>(Map.class));
    }

    // Session

    public static Serde<SessionIn> sessionIn() {
        return Serdes.serdeFrom(new JsonSerializer<>(),
                new JsonDeserializer<>(SessionIn.class));
    }

    public static Serde<SessionData> sessionOut() {
        return Serdes.serdeFrom(new JsonSerializer<>(),
                new JsonDeserializer<>(SessionData.class));
    }
}
