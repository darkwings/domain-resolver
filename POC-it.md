# POC TIM

## KSQL

Se si usa CLI KSQL, facciamo in modo che le query partano dall'inizio dei topic

    SET 'auto.offset.reset' = 'earliest';

Creiamo i topic necessari.

    bin/kafka-topics --create --topic cmpny_users --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_domains --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_enriched --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_dlq --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_retry_1 --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092

Vanilla Kafka

    bin/kafka-topics.sh --create --topic cmpny_users --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics.sh --create --topic cmpny_activity --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics.sh --create --topic cmpny_domains --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics.sh --create --topic cmpny_activity_enriched --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092    
    bin/kafka-topics.sh --create --topic cmpny_activity_dlq --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics.sh --create --topic cmpny_activity_retry_1 --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092


### GeoIP

La UDF (User Defined Function) utilizzata è disponibile al link https://github.com/darkwings/ksql-geoip. Per creare il jar eseguire

    mvn clean package


La UDF utilizza il DB Geolite di MaxMind. Scaricare e posizionare il file sull'host ksqldb.

Aggiungere al file di configurazione di ksqldb (per Confluent si trova in `$CONFLUENT_HOME/etc/ksqldb/ksql-server.properties`) 
le seguenti properties:

    ksql.functions._global_.geocity.db.path=/path/to/GeoLite2-City.mmdb
    ksql.extension.dir=/path/to/extensions

Nella directory identificata da `ksql.extension.dir` copiare il jar della UDF.


## USERS

Creare lo stream KSQL e la tabella che materializza i dati dello stream

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


Questi dati dovrebbero arrivare da connettore, da CDC o da fonti esterne, tipo LDAP.

### CREAZIONE UTENTI

Popolare lo stream. Eseguire da KSQL shell o dalla CLI KSQL

    insert into cmpny_users (userid, cn, roomNumber, email) values ('111', 'Bill Flanagan', '201', 'bill.flanagan@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('222', 'Joe Doe', '202', 'joe.doe@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('333', 'John Slow', '203', 'john.slow@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('444', 'David Flanagan', '204', 'david.flanagan@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('555', 'Sam Mendes', '205', 'sam.mendes@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('444', 'David Flanagan', '206', 'david.flanagan@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('222', 'Joe Doe', '310', 'joe.doe@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('666', 'Lucifer Devil', '341', 'lucifer.devil@cmpny.com');


E' anche possibile scrivere direttamente sul topic tramite console

    bin/kafka-console-producer --topic cmpny_users --bootstrap-server localhost:9092 --property parse.key=true --property key.separator=":"
    > 666:{"cn":"Lucifer Devil","roomNumber":"290","email":"lucifer.devil@cmpny.com"}


## ACTIVITY


Creare uno stream che è alimentato dal topic su cui verrà pushata l'attività dell'utente

    CREATE STREAM cmpny_activity (activityid VARCHAR KEY, userid VARCHAR, message VARCHAR, date VARCHAR, ip VARCHAR, domain VARCHAR)
      WITH (kafka_topic='cmpny_activity', value_format='json', partitions=4);


Creare l'enriched stream che utilizza la funzione getDataFromIp(). I dati dell'utente arrivano da una join con 
la vista materializzata. I dati del GeoIP dalla User-Defined Function.

GepIP Lite non permette di risalire al dominio, per cui viene passato manualmente.

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

A questo punto eseguire la push query

    SELECT * FROM cmpny_activity_with_location_user EMIT CHANGES;

N.B. Nel topic cmpny_activity_with_location_user la key è userid




## Domain Resolution

Scaricare il progetto https://github.com/darkwings/domain-resolver e compilare con

    mvn clean package

Per eseguire due istanze

    java -Dapi.key=AIzaSyAWAvWykwTIl90nG4kmxlWA7u5BHzmXMls -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config.properties

    java -Dapi.key=AIzaSyAWAvWykwTIl90nG4kmxlWA7u5BHzmXMls -jar target/domain-resolution-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./conf/config2.properties

Modificare i file `config.properties` e `config2.properties` per modificare la posizione dello state store.



## Verifica enrichment

In una shell è possibile verificare i dati finali arricchiti

    bin/kafka-console-consumer --topic cmpny_activity_enriched --bootstrap-server localhost:9092 --property print.key=true


In un altra shell KSQL, popolare lo stream cmpny_activity per vedere la push query sopra che si aggiorna con i dati arricchiti

    insert into cmpny_activity(activityid, userid, message, ip, domain, date) values ('3o4871529', '111', 'Some message 1', '142.250.180.131', 'google.com', '2022-02-25T15:54:23.025627Z');
    insert into cmpny_activity(activityid, userid, message, ip, domain, date) values ('3452345234', '111', 'Some message 2', '87.6.3.92', 'stellantis.com', '2022-02-25T15:54:23.025627Z');

Oppure pubblicare direttamente sul topic cmpny_activity

    bin/kafka-console-producer --topic cmpny_activity --bootstrap-server localhost:9092 --property parse.key=true --property key.separator=":"
    > 52345234:{"userid":"333","message":"Some message 10","ip":"142.250.180.131", "domain":"google.com", "date":"2022-02-25T15:54:23.025627Z"}

### Custom producer

E' disponibile anche un tool per la produzione massiva di dati. Si trova qui: https://github.com/darkwings/domain-resolution-producer.

Per eseguire localmente, compilare con `mvn clean package` e avviare con

    java -Dbootstrap.servers=localhost:9092 -jar target/kafka-producer-0.0.1-SNAPSHOT.jar

Per generare i dati degli utenti

    curl -X POST http://localhost:8080/bulk/users/cmpny_users/1000

Per generare messaggi 

    curl -X POST http://localhost:8080/bulk/activity/cmpny_activity/15/5     (30000 mess/sec)

    curl -X POST http://localhost:8080/bulk/activity/cmpny_activity/20/10    (20000 mess/sec)

Vedere il README del progetto per tutte le opzioni.


## Riassumendo

Per fare copy&paste veloce.

### Topics

Dal terminale

    bin/kafka-topics --create --topic cmpny_users --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_domains --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_enriched --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_dlq --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
    bin/kafka-topics --create --topic cmpny_activity_retry_1 --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092

### KSQL

Dal Control Center, o dalla CLI

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


    insert into cmpny_users (userid, cn, roomNumber, email) values ('111', 'Bill Flanagan', '201', 'bill.flanagan@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('222', 'Joe Doe', '202', 'joe.doe@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('333', 'John Slow', '203', 'john.slow@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('444', 'David Flanagan', '204', 'david.flanagan@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('555', 'Sam Mendes', '205', 'sam.mendes@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('444', 'David Flanagan', '206', 'david.flanagan@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('222', 'Joe Doe', '310', 'joe.doe@cmpny.com');
    insert into cmpny_users (userid, cn, roomNumber, email) values ('666', 'Lucifer Devil', '341', 'lucifer.devil@cmpny.com');  