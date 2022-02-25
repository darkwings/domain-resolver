package com.nttdata.poc.model;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@FieldDefaults(level = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ActivityEnriched {

    @SerializedName(value = "ACTIVITY")
    Activity activity;

    @SerializedName(value = "SUSPECT")
    Boolean suspect;

    public void mark(Boolean suspect) {
        this.suspect = suspect;
    }
}
