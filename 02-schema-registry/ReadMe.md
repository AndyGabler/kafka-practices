# Schema Registry App

This sub-folder contains a =consumer app and a basic producer app. Goal of this was to demonstrate the
usage of Kafka Schema Registry to myself.

Both apps connect to a local Kafka instance running at `localhost:9092` and interact with a topic named 
`nfl.game-results.v3`.

The Producer will take a Spring MVC form of NFL game wins. The Consumer will dump these wins in an
H2 in-memory database and simply serve them as a REST API.

## Architecture

![Schema Registry Demo Architecture](./docs/Schema%20Registry%20Demo%20Architecture.drawio.png)

## Setup

First, you will need to set up the Kafka environment on your local machine. To do this, open a command prompt in the
`docker-kafka-deploy` directory and run
```
docker-compose up -d
```

## Running Programs

To run the producer, simply run
```
gradlew :nfl-result-producer:run
```

To run the consumer, simply run
```
gradlew :nfl-result-consumer:run
```

## Schema Setup

The producer application will automatically create a schema upon the creation of the first producer record. The Producer
is responsible for maintaining a schema.

After navigating to `http://localhost:8080/gameResult` and submitting a game result, a schema will appear on schema
registry at `http://localhost:8081/schemas`. This is the schema produced by this application.
```json
[
  {
    "subject": "nfl.game.result.v3-value",
    "version": 1,
    "id": 1,
    "guid": "d5e5f9fc-abc5-cbb2-bf62-d41837bba002",
    "schemaType": "JSON",
    "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Game Result\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"homeTeam\":{\"type\":\"string\",\"enum\":[\"Bears\",\"Bengals\",\"Bills\",\"Broncos\",\"Browns\",\"Buccaneers\",\"Cardinals\",\"Chargers\",\"Chiefs\",\"Colts\",\"Commanders\",\"Cowboys\",\"Dolphins\",\"Eagles\",\"Falcons\",\"FortyNiners\",\"Giants\",\"Jaguars\",\"Jets\",\"Lions\",\"Packers\",\"Panthers\",\"Patriots\",\"Raiders\",\"Rams\",\"Ravens\",\"Saints\",\"Seahawks\",\"Steelers\",\"Texans\",\"Titans\",\"Vikings\"]},\"homeTeamScore\":{\"type\":\"integer\"},\"visitingTeam\":{\"type\":\"string\",\"enum\":[\"Bears\",\"Bengals\",\"Bills\",\"Broncos\",\"Browns\",\"Buccaneers\",\"Cardinals\",\"Chargers\",\"Chiefs\",\"Colts\",\"Commanders\",\"Cowboys\",\"Dolphins\",\"Eagles\",\"Falcons\",\"FortyNiners\",\"Giants\",\"Jaguars\",\"Jets\",\"Lions\",\"Packers\",\"Panthers\",\"Patriots\",\"Raiders\",\"Rams\",\"Ravens\",\"Saints\",\"Seahawks\",\"Steelers\",\"Texans\",\"Titans\",\"Vikings\"]},\"visitingTeamScore\":{\"type\":\"integer\"},\"datePlayed\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"integer\",\"format\":\"utc-millisec\"}]}},\"required\":[\"homeTeam\",\"homeTeamScore\",\"visitingTeam\",\"visitingTeamScore\"]}"
  }
]
```

## Demonstration

This demonstration will show that:
 * The Kafka Schema Registry is working
 * The producer generates Schema records
 * The consumer tries to serialize with the Schema
 * How a bad record is handled

To start with, a local Docker image for Kafka and Schema registry must be setup. Instructions in setup will get us this
far.

From there, if we run the Producer application, we can navigate to its endpoint at 
[http://localhost:8080/gameResult](http://localhost:8080/gameResult). This will bring up the following form.

![Game-Result-Form](/docs/demo/01-game-result-form.png)

This form is used to submit the result of an NFM game to the Kafka topic `nfl.game-results.v3`. It will do so with
Schema registy in mind. When this setup is first done, if we navigate to Schema registry to search for Schemas at 
[http://localhost:8081/schemas](http://localhost:8081/schemas), the Json returned should simply be
```json
[]
```

However, after filling out the form, if we repeat that process there should be the schema depicted in the "Schema Setup"
section. You will know if the record was produced successfully if you receive the application log from the producer:
```
2026-01-21T19:36:08.434-05:00  INFO 33140 --- [nfl-result-producer] [nio-8080-exec-1] i.g.a.n.r.p.g.GameResultController       : Game result form loaded.
2026-01-21T19:40:32.600-05:00  INFO 33140 --- [nfl-result-producer] [nio-8080-exec-4] i.g.a.n.r.p.g.GameResultService          : Sending result of Broncos @ Bills to Kafka topic.
2026-01-21T19:40:32.906-05:00  INFO 33140 --- [nfl-result-producer] [nio-8080-exec-4] i.g.a.n.r.p.g.GameResultController       : Game result form loaded.
```

The main thing to demonstrate here is the Consumer. If you run the consumer, you can navigate to its endpoints to return
the game results at [http://localhost:8085/gameResults](http://localhost:8085/gameResults).

Something to note here is that it may not return results immediately. This is due to a quirk with its first poll of the
topic. This architecture is not what you would use in a Production environment but this project is to solely demonstrate 
Schema Registry. The first poll should return a result like this.
```
2026-01-21T19:42:12.226-05:00  INFO 7756 --- [nfl-result-consumer] [nio-8085-exec-1] i.g.a.n.r.c.k.ConsumeGameResultService   : Attempting to update game results. Starting initial poll.
2026-01-21T19:42:12.722-05:00  INFO 7756 --- [nfl-result-consumer] [nio-8085-exec-1] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-nfl-result-consumer-084674f7-8a8f-4dce-856c-570715f2db3e-1, groupId=nfl-result-consumer-084674f7-8a8f-4dce-856c-570715f2db3e] Cluster ID: MkU3OEVBNTcwNTJENDM2Qk
2026-01-21T19:42:12.723-05:00  INFO 7756 --- [nfl-result-consumer] [nio-8085-exec-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-nfl-result-consumer-084674f7-8a8f-4dce-856c-570715f2db3e-1, groupId=nfl-result-consumer-084674f7-8a8f-4dce-856c-570715f2db3e] Discovered group coordinator localhost:9092 (id: 2147483646 rack: null)
2026-01-21T19:42:12.731-05:00  INFO 7756 --- [nfl-result-consumer] [nio-8085-exec-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-nfl-result-consumer-084674f7-8a8f-4dce-856c-570715f2db3e-1, groupId=nfl-result-consumer-084674f7-8a8f-4dce-856c-570715f2db3e] (Re-)joining group
2026-01-21T19:42:12.745-05:00  INFO 7756 --- [nfl-result-consumer] [nio-8085-exec-1] i.g.a.n.r.c.GameResultLookupController   : Game results updated. Querying.
```

The second poll however should produce some records that get saved to the database if we make the GET request again.
```
2026-01-21T19:43:57.432-05:00  INFO 7756 --- [nfl-result-consumer] [nio-8085-exec-7] o.a.k.c.c.internals.SubscriptionState    : [Consumer clientId=consumer-nfl-result-consumer-084674f7-8a8f-4dce-856c-570715f2db3e-1, groupId=nfl-result-consumer-084674f7-8a8f-4dce-856c-570715f2db3e] Resetting offset for partition nfl.game.result.v3-0 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1 rack: null)], epoch=0}}.
2026-01-21T19:43:57.707-05:00  INFO 7756 --- [nfl-result-consumer] [nio-8085-exec-7] i.g.a.n.r.c.k.ConsumeGameResultService   : Found new message.
	Topic = nfl.game.result.v3
	Partition = 0
	Key = Rams @ Bears
2026-01-21T19:43:57.772-05:00  INFO 7756 --- [nfl-result-consumer] [nio-8085-exec-7] i.g.a.n.r.c.k.ConsumeGameResultService   : Found new message.
	Topic = nfl.game.result.v3
	Partition = 0
	Key = Broncos @ Bills
2026-01-21T19:43:57.774-05:00  INFO 7756 --- [nfl-result-consumer] [nio-8085-exec-7] i.g.a.n.r.c.GameResultLookupController   : Game results updated. Querying.
```

This should indicate it has consumed from the topics. The JSON result should look something like this.
```json
[
  {
    "homeTeam": "Bears",
    "homeTeamScore": 17,
    "id": 1,
    "visitingTeam": "Rams",
    "visitingTeamScore": 20
  },
  {
    "homeTeam": "Bills",
    "homeTeamScore": 30,
    "id": 2,
    "visitingTeam": "Broncos",
    "visitingTeamScore": 33
  }
]
```

Another note on the consumer's architecture is that it changes its consumer group ID every time. This way it loads
all messages every start. This is not something that you would do in a Production environment (obviously) but it's a
quirk we're accepting for its model of having an H2 in-memory database.

To demonstrate what happens when we produce a bad record, we can simply run the Python script `produce-bad-record.py`.
This produces a record to the topic that does not match the schema.

When we hit the endpoint on the consumer again, we get
```
2026-01-21T19:48:02.531-05:00 ERROR 7756 --- [nfl-result-consumer] [io-8085-exec-10] i.g.a.n.r.c.k.ConsumeGameResultService   : Failure in polling for records. Committing offset to skip. 

org.apache.kafka.common.errors.RecordDeserializationException: Error deserializing VALUE for partition nfl.game.result.v3-0 at offset 2. If needed, please seek past the record to continue consumption.
	at org.apache.kafka.clients.consumer.internals.CompletedFetch.newRecordDeserializationException(CompletedFetch.java:346) ~[kafka-clients-3.9.1.jar:na]
	at org.apache.kafka.clients.consumer.internals.CompletedFetch.parseRecord(CompletedFetch.java:330) ~[kafka-clients-3.9.1.jar:na]
	at org.apache.kafka.clients.consumer.internals.CompletedFetch.fetchRecords(CompletedFetch.java:284) ~[kafka-clients-3.9.1.jar:na]
	at org.apache.kafka.clients.consumer.internals.FetchCollector.fetchRecords(FetchCollector.java:169) ~[kafka-clients-3.9.1.jar:na]
	at org.apache.kafka.clients.consumer.internals.FetchCollector.collectFetch(FetchCollector.java:135) ~[kafka-clients-3.9.1.jar:na]
	at org.apache.kafka.clients.consumer.internals.Fetcher.collectFetch(Fetcher.java:146) ~[kafka-clients-3.9.1.jar:na]
	at org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer.pollForFetches(ClassicKafkaConsumer.java:699) ~[kafka-clients-3.9.1.jar:na]
	at org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer.poll(ClassicKafkaConsumer.java:623) ~[kafka-clients-3.9.1.jar:na]
	at org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer.poll(ClassicKafkaConsumer.java:596) ~[kafka-clients-3.9.1.jar:na]
	at org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:874) ~[kafka-clients-3.9.1.jar:na]
	at io.github.andygabler.nfl.result.consumer.kafka.ConsumeGameResultService.updateGameResults(ConsumeGameResultService.java:46) ~[main/:na]
	at io.github.andygabler.nfl.result.consumer.GameResultLookupController.getGameResults(GameResultLookupController.java:28) ~[main/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
	at org.springframework.web.method.support.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:258) ~[spring-web-7.0.1.jar:7.0.1]
	at org.springframework.web.method.support.InvocableHandlerMethod.invokeForRequest(InvocableHandlerMethod.java:190) ~[spring-web-7.0.1.jar:7.0.1]
	at org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod.invokeAndHandle(ServletInvocableHandlerMethod.java:117) ~[spring-webmvc-7.0.1.jar:7.0.1]
	at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.invokeHandlerMethod(RequestMappingHandlerAdapter.java:934) ~[spring-webmvc-7.0.1.jar:7.0.1]
	at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.handleInternal(RequestMappingHandlerAdapter.java:853) ~[spring-webmvc-7.0.1.jar:7.0.1]
	at org.springframework.web.servlet.mvc.method.AbstractHandlerMethodAdapter.handle(AbstractHandlerMethodAdapter.java:86) ~[spring-webmvc-7.0.1.jar:7.0.1]
	at org.springframework.web.servlet.DispatcherServlet.doDispatch(DispatcherServlet.java:963) ~[spring-webmvc-7.0.1.jar:7.0.1]
	at org.springframework.web.servlet.DispatcherServlet.doService(DispatcherServlet.java:866) ~[spring-webmvc-7.0.1.jar:7.0.1]
	at org.springframework.web.servlet.FrameworkServlet.processRequest(FrameworkServlet.java:1003) ~[spring-webmvc-7.0.1.jar:7.0.1]
	at org.springframework.web.servlet.FrameworkServlet.doGet(FrameworkServlet.java:892) ~[spring-webmvc-7.0.1.jar:7.0.1]
	at jakarta.servlet.http.HttpServlet.service(HttpServlet.java:622) ~[tomcat-embed-core-11.0.14.jar:6.1]
	at org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:874) ~[spring-webmvc-7.0.1.jar:7.0.1]
	at jakarta.servlet.http.HttpServlet.service(HttpServlet.java:710) ~[tomcat-embed-core-11.0.14.jar:6.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:128) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:53) ~[tomcat-embed-websocket-11.0.14.jar:11.0.14]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:107) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.springframework.web.filter.RequestContextFilter.doFilterInternal(RequestContextFilter.java:100) ~[spring-web-7.0.1.jar:7.0.1]
	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116) ~[spring-web-7.0.1.jar:7.0.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:107) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.springframework.web.filter.FormContentFilter.doFilterInternal(FormContentFilter.java:93) ~[spring-web-7.0.1.jar:7.0.1]
	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116) ~[spring-web-7.0.1.jar:7.0.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:107) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:199) ~[spring-web-7.0.1.jar:7.0.1]
	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116) ~[spring-web-7.0.1.jar:7.0.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:107) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:165) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:77) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:482) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:113) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:83) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:72) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:341) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.coyote.http11.Http11Processor.service(Http11Processor.java:397) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.coyote.AbstractProcessorLight.process(AbstractProcessorLight.java:63) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.coyote.AbstractProtocol$ConnectionHandler.process(AbstractProtocol.java:903) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1778) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.tomcat.util.net.SocketProcessorBase.run(SocketProcessorBase.java:52) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.tomcat.util.threads.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:946) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.tomcat.util.threads.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:480) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run(TaskThread.java:57) ~[tomcat-embed-core-11.0.14.jar:11.0.14]
	at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]
Caused by: org.apache.kafka.common.errors.SerializationException: Error deserializing JSON message for id SchemaId{schemaType='JSON', id=null, guid='null', messageIndexes=null}
	at io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaDeserializer.deserialize(AbstractKafkaJsonSchemaDeserializer.java:240) ~[kafka-json-schema-serializer-8.0.0.jar:na]
	at io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer.deserialize(KafkaJsonSchemaDeserializer.java:88) ~[kafka-json-schema-serializer-8.0.0.jar:na]
	at org.apache.kafka.common.serialization.Deserializer.deserialize(Deserializer.java:81) ~[kafka-clients-3.9.1.jar:na]
	at org.apache.kafka.clients.consumer.internals.CompletedFetch.parseRecord(CompletedFetch.java:327) ~[kafka-clients-3.9.1.jar:na]
	... 55 common frames omitted
Caused by: org.apache.kafka.common.errors.SerializationException: Error deserializing schema ID
	at io.confluent.kafka.serializers.schema.id.DualSchemaIdDeserializer.deserialize(DualSchemaIdDeserializer.java:49) ~[kafka-schema-serializer-8.0.0.jar:na]
	at io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaDeserializer.deserialize(AbstractKafkaJsonSchemaDeserializer.java:132) ~[kafka-json-schema-serializer-8.0.0.jar:na]
	... 58 common frames omitted
Caused by: java.lang.IllegalArgumentException: Unknown magic byte!
	at io.confluent.kafka.serializers.schema.id.SchemaId.fromBytes(SchemaId.java:70) ~[kafka-schema-serializer-8.0.0.jar:na]
	at io.confluent.kafka.serializers.schema.id.DualSchemaIdDeserializer.deserialize(DualSchemaIdDeserializer.java:47) ~[kafka-schema-serializer-8.0.0.jar:na]
	... 59 common frames omitted
```

This is known as a "Poison Pill" since it does not conform to the Schema. To get past this, the consumer seeks to the
end of the partition it is assigned to.

That said, that does verify the consumer will only pick up messages matching the schema!