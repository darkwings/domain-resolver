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
public class Location {

    @SerializedName(value = "CITY")
    String city;

    @SerializedName(value = "STATE")
    String state;

    @SerializedName(value = "LATITUDE")
    Double latitude;

    @SerializedName(value = "LONGITUDE")
    Double longitude;
}
