# DOMAIN RESOLVER KAFKA STREAMS APP

## TOPICS

    bin/kafka-topics --create --topic cmpny_users --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092 
    bin/kafka-topics --create --topic cmpny_domains --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_enriched --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_dlq --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_retry_1 --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092

    bin/kafka-topics --create --topic session_in --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic session_out --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092

    bin/kafka-topics --create --topic generic_in --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic generic_out --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092

Then follow instructions on KSQL.md file to prepare KSQL path.

You'll also need the KSQL UDF available in https://github.com/darkwings/ksql-geoip.

## Execution
    
    java -Dapi.key=safebrowsing_api_key -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config.properties
    
To start another instance, just provide a configuration file with a different state directory 

    java -Dapi.key=safebrowsing_api_key -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config2.properties

Notice that `safebrowsing_api_key` should be a valid API KEY to access the Google Safe Browsing service.

## Prometheus metrics

TBD

Download the exporter from https://github.com/prometheus/jmx_exporter and follow the instructions


## To start

Compile with

     mvn clean package


### Without Prometheus exporter

     java -Dapi.key=AIzaSyAWAvWykwTIl90nG4kmxlWA7u5BHzmXMls -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config.properties

     java -Dapi.key=AIzaSyAWAvWykwTIl90nG4kmxlWA7u5BHzmXMls -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config2.properties

### With Prometheus exporter

TBD

     java -Dapi.key=AIzaSyAWAvWykwTIl90nG4kmxlWA7u5BHzmXMls -javaagent:./prometheus/jmx_prometheus_javaagent-0.16.1.jar=9081:config.yaml -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config.properties

     java -Dapi.key=AIzaSyAWAvWykwTIl90nG4kmxlWA7u5BHzmXMls -javaagent:./prometheus/jmx_prometheus_javaagent-0.16.1.jar=9082:config.yaml -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config2.properties

