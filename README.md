# DOMAIN RESOLVER KAFKA STREAMS APP

## TOPICS

    bin/kafka-topics --create --topic cmpny_users --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092 
    bin/kafka-topics --create --topic cmpny_domains --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_enriched --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_dlq --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092


## Execution
    
    java -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar localhost:9092 CMPNY_ACTIVITY_WITH_LOCATION_USER cmpny_activity_enriched cmpny_domains /tmp/state-12 api_key_safebrowsing
    
    java -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar localhost:9092 CMPNY_ACTIVITY_WITH_LOCATION_USER cmpny_activity_enriched cmpny_domains /tmp/state-121 api_key_safebrowsing


## Prometheus metrics

Download the exporter from https://github.com/prometheus/jmx_exporter and follow the instructions

## To start

     mvn clean package

### With Prometheus exporter

     java -Dapi.key=AIzaSyAWAvWykwTIl90nG4kmxlWA7u5BHzmXMls -javaagent:./prometheus/jmx_prometheus_javaagent-0.16.1.jar=9081:config.yaml -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config.properties

     java -Dapi.key=AIzaSyAWAvWykwTIl90nG4kmxlWA7u5BHzmXMls -javaagent:./prometheus/jmx_prometheus_javaagent-0.16.1.jar=9082:config.yaml -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config2.properties

### Without Prometheus exporter

     java -Dapi.key=AIzaSyAWAvWykwTIl90nG4kmxlWA7u5BHzmXMls -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config.properties

     java -Dapi.key=AIzaSyAWAvWykwTIl90nG4kmxlWA7u5BHzmXMls -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config2.properties