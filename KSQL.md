# KSQL DB Preparation

## KSQL Streams and Tables

When topics are created, create the following stream and tables

    CREATE OR REPLACE STREAM cmpny_users (userid VARCHAR KEY, cn VARCHAR, roomNumber VARCHAR, email VARCHAR)
      WITH (kafka_topic='cmpny_users', value_format='json', partitions=4);


    CREATE TABLE cmpny_users_by_id AS
      SELECT userid,
           latest_by_offset(cn) AS cn,
           latest_by_offset(roomNumber) AS roomNumber,
           latest_by_offset(email) AS email
      FROM cmpny_users
      GROUP BY userid
      EMIT CHANGES; 


    CREATE STREAM cmpny_activity (activityid VARCHAR KEY, userid VARCHAR, message VARCHAR, date VARCHAR, ip VARCHAR, domain VARCHAR)
      WITH (kafka_topic='cmpny_activity', value_format='json', partitions=4);


    CREATE STREAM cmpny_activity_with_location_user AS
      SELECT a.activityid,
           a.userid as userid,
           a.message,
           a.date,
           a.ip,
           getDataFromIp(a.ip) as location,
           a.domain,
           u.cn,
           u.roomNumber,
           u.email
      FROM cmpny_activity AS a
      LEFT JOIN cmpny_users_by_id u
      ON u.userid = a.userid
      EMIT CHANGES;

Now you can insert some users. 

### New users via KSQL

From the ksql cli or the Control Center, if available:

    insert into cmpny_users (userid, cn, roomNumber, email) values ('111', 'Bill Flanagan', '201', 'bill.flanagan@cmpny.com');

### New users with the console producer script

First, start the console producer

    ./kafka-console-producer --topic cmpny_users --bootstrap-server localhost:9092 --property parse.key=true --property key.separator=":"

At the prompt, you can insert something like that

    > 666:{"cn":"Lucifer Devil","roomNumber":"290","email":"lucifer.devil@cmpny.com"}

With vanilla Kafka, the script is `kafka-console-producer.sh`.

## Activity

To insert some activities (logs) you have two options

### Activities via KSQL

From the ksql cli or the Control Center, if available:

    insert into cmpny_activity(activityid, userid, message, ip, domain, date) values ('98234751293567', '111', 'Some message 1', '142.250.180.131', 'google.com', '2022-02-25T15:54:23.025627Z');

### Activities with the console producer script

First, start the console producer

    ./kafka-console-producer --topic cmpny_activity --bootstrap-server localhost:9092 --property parse.key=true --property key.separator=":"

At the prompt, you can insert something like that

    > 98234751293567:{userid":"111","message":"Some message 10","ip":"142.250.180.131", "domain":"google.com", "date":"2022-02-25T15:54:23.025627Z"}

With vanilla Kafka, the script is `kafka-console-producer.sh`.