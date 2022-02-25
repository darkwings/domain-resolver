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
public class Activity {

    @SerializedName(value = "ACTIVITYID")
    String activityId;

    @SerializedName(value = "USERID")
    String userId;

    @SerializedName(value = "CN")
    String commonName;

    @SerializedName(value = "ROOMNUMBER")
    String roomNumber;

    @SerializedName(value = "EMAIL")
    String email;

    @SerializedName(value = "IP")
    String ip;

    @SerializedName(value = "DOMAIN")
    String domain;

    @SerializedName(value = "LOCATION")
    Location location;

    @SerializedName(value = "MESSAGE")
    String message;

    @SerializedName(value = "DATE")
    String timestamp;

    @SerializedName(value = "ACTION")
    String action;
}
