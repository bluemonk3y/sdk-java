/**
 * Copyright 2019 The CloudEvents Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cloudevents.v03.kafka;

import io.cloudevents.CloudEvent;
import io.cloudevents.types.Much;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.CloudEventBuilder;
import io.cloudevents.v03.CloudEventImpl;
import io.debezium.junit.SkipLongRunning;
import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.test.StreamsTestUtils.getStreamsConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 */
public class CEHeader_KafkaProducerBinaryTest {
  private static final Logger log = LoggerFactory.getLogger(CEHeader_KafkaProducerBinaryTest.class);

  int zooKeeperPort = 12000;
  int brokerPort = 11000;

  private static final int ONE_BROKER = 1;
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private KafkaCluster kafka;
  private File data;

  @BeforeEach
  public void beforeEach() throws IOException {
    data = Testing.Files.createTestingDirectory("cluster");


    kafka = new KafkaCluster()
            .usingDirectory(data)
            .deleteDataPriorToStartup(true)
            .deleteDataUponShutdown(true)
            .withPorts(zooKeeperPort, brokerPort);

    kafka.addBrokers(ONE_BROKER).startup();

  }

  @AfterEach
  public void afterEach() {
    kafka.shutdown();
    Testing.Files.delete(data);
  }


  @Test
  @SkipLongRunning
  public void passManadatoryCeAttributesFromProducerToConsumer() throws Exception {
    // setup

    CloudEventImpl<Much> ce = getMuchCloudEvent("nice!");

    final String topic = "binary.t";

    kafka.createTopics(topic);

    Properties producerProperties = kafka.useTo().getProducerProperties("bin.me");

    Properties consumerProperties = kafka.useTo()
            .getConsumerProperties("consumer", "consumer.id", OffsetResetStrategy.EARLIEST);


    try (
            KafkaProducer<String, CloudEvent<AttributesImpl, Much>> ceProducer = new KafkaProducer<String, CloudEvent<AttributesImpl, Much>>(producerProperties, new StringSerializer(), new CeHeaderBinarySerializer())) {

      ProducerRecord<String, CloudEvent<AttributesImpl, Much>> record = new CeHeaderBinarySerializer().getProducerRecordWithCloudEvent(topic, ce);
      RecordMetadata metadata = ceProducer.send(record).get();

      log.info("Producer metadata {}", metadata);
    }

    try (KafkaConsumer<String, Much> consumer = new KafkaConsumer<>(consumerProperties,
            new StringDeserializer(), new CeHeaderBinaryDeserializer<>(Much.class))) {

      consumer.subscribe(Collections.singletonList(topic));

      ConsumerRecords<String, Much> records = consumer.poll(TIMEOUT);

      ConsumerRecord<String, Much> actual = records.iterator().next();

      assertGoodness(ce, actual);
    }
  }



  @Test
  @SkipLongRunning
  public void passCeAttributesThroughKStreamsFilter() throws Exception {
    // setup

    CloudEventImpl<Much> ce = getMuchCloudEvent("streaming!");

    final String testTopic = "cloud.events.binary.topic.test";
    final String outputTopic = "cloud.events.binary.topic.streamed.topic";

    kafka.createTopics(testTopic);

    Properties producerProperties = kafka.useTo().getProducerProperties("bin.me");

    Properties consumerProperties = kafka.useTo().getConsumerProperties("consumer", "consumer.id", OffsetResetStrategy.EARLIEST);

    try (
            KafkaProducer<String, CloudEvent<AttributesImpl, Much>> ceProducer = new KafkaProducer<String, CloudEvent<AttributesImpl, Much>>(producerProperties, new StringSerializer(), new CeHeaderBinarySerializer())) {

      ProducerRecord<String, CloudEvent<AttributesImpl, Much>> record = new CeHeaderBinarySerializer().getProducerRecordWithCloudEvent(testTopic, ce);
      RecordMetadata metadata = ceProducer.send(record).get();
      ceProducer.flush();

      log.info("Meta:" + metadata);
    }

    /**
     * Filtering KStreamsTopology
     */
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Much> inflight = builder.stream(testTopic, Consumed.with(new Serdes.StringSerde(), new CeHeaderBinarySerDes<>(Much.class)));

    inflight.filter((k,v) -> {
      System.out.println("processing cloud events");
      return v.getWow().contains("streaming");
    }).to(outputTopic, Produced.with(new Serdes.StringSerde(), new CeHeaderBinarySerDes<>(Much.class)));

    Topology topology = builder.build();
    System.out.println(topology.describe());
    KafkaStreams streams = new KafkaStreams(topology, getStreamsConfigForTest());
    streams.start();

    Thread.sleep(1000);


    try (KafkaConsumer<String, Much> consumer = new KafkaConsumer<>(consumerProperties,
            new StringDeserializer(), new CeHeaderBinaryDeserializer<>(Much.class))) {

      consumer.subscribe(Collections.singletonList(outputTopic));

      ConsumerRecords<String, Much> records = consumer.poll(TIMEOUT);

      System.out.println("============================= DONE =============================");

      ConsumerRecord<String, Much> actual = records.iterator().next();


      assertGoodness(ce, actual);
    }
  }


  @Test
  @SkipLongRunning
  public void passCeAttributesThroughKStreamsAggregate() throws Exception {
    // setup
    CloudEventImpl<Much> ce = getMuchCloudEvent("streaming!");

    final String testTopic = "cloud.events.binary.topic.test";
    final String outputTopic = "cloud.events.binary.topic.streamed.topic";

    Properties producerProperties = kafka.useTo().getProducerProperties("bin.me");

    Properties consumerProperties = kafka.useTo().getConsumerProperties("consumer", "consumer.id", OffsetResetStrategy.EARLIEST);

    try (
            KafkaProducer<String, CloudEvent<AttributesImpl, Much>> ceProducer = new KafkaProducer<String, CloudEvent<AttributesImpl, Much>>(producerProperties, new StringSerializer(), new CeHeaderBinarySerializer())) {

      ProducerRecord<String, CloudEvent<AttributesImpl, Much>> record = getProducerRecordWithCloudEvent(testTopic, ce);
//      ceProducer.send(record).get();
//      ceProducer.send(record).get();
      RecordMetadata metadata = ceProducer.send(record).get();
      ceProducer.flush();

      log.info("Meta:" + metadata);
    }

    /**
     * Reducing KStreamsTopology
     */
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Much> inflight = builder.stream(testTopic, Consumed.with(new Serdes.StringSerde(), new CeHeaderBinarySerDes<>(Much.class)));

    KGroupedStream<String, Much> groupedStream = inflight.groupByKey();
    KTable<String, Much> reduce = groupedStream.reduce((value1, value2) -> {
      System.out.println("\n\t\t\t ========= Reducing:" + value1);
      return value2;
    });
    reduce.toStream().to(outputTopic, Produced.with(new Serdes.StringSerde(), new CeHeaderBinarySerDes<>(Much.class)));

    Topology topology = builder.build();
    System.out.println(topology.describe());
    KafkaStreams streams = new KafkaStreams(topology, getStreamsConfigForTest());
    streams.start();

    Thread.sleep(1000);


    try (KafkaConsumer<String, Much> consumer = new KafkaConsumer<>(consumerProperties,
            new StringDeserializer(), new CeHeaderBinaryDeserializer<>(Much.class))) {

      consumer.subscribe(Collections.singletonList(outputTopic));

      ConsumerRecords<String, Much> records = consumer.poll(TIMEOUT);

      System.out.println("============================= DONE =============================");

      ConsumerRecord<String, Much> actual = records.iterator().next();
      assertGoodness(ce, actual);


    }
  }

  private void assertGoodness(CloudEventImpl<Much> ce, ConsumerRecord<String, Much> actual) throws URISyntaxException {
    // assert
    assertNotNull(actual);

    System.out.println("got:" + actual);
    Header specversion = actual.headers().lastHeader("ce_specversion");

    assertNotNull(specversion);
    assertEquals(ce.getAttributes().getSpecversion(), new String(specversion.value()));

    Header id = actual.headers().lastHeader("ce_id");
    assertNotNull(id);
    assertEquals(ce.getAttributes().getId(), new String(id.value()));

    Header source = actual.headers().lastHeader("ce_source");
    assertNotNull(source);
    assertEquals(ce.getAttributes().getSource(), new URI(new String(source.value())));

    Header type = actual.headers().lastHeader("ce_type");
    assertNotNull(source);
    assertEquals(ce.getAttributes().getType(), new String(type.value()));

    Header subject = actual.headers().lastHeader("ce_subject");
    assertNotNull(subject);
    assertEquals(ce.getAttributes().getSubject().get(), new String(subject.value()));

    Much actualData = actual.value();
    assertNotNull(actualData);
    assertEquals(ce.getData().get(), actualData);
  }

  private CloudEventImpl<Much> getMuchCloudEvent(String s) {
    return CloudEventBuilder.<Much>builder()
            .withId("x10")
            .withSource(URI.create("/source"))
            .withType("event-type")
            .withDatacontenttype("application/json")
            .withSubject("subject")
            .withData(new Much(s))
            .build();
  }


  private ProducerRecord<String, CloudEvent<AttributesImpl, Much>> getProducerRecordWithCloudEvent(String topic, CloudEventImpl<Much> ce) {
      return new ProducerRecord<>(topic, ce.getAttributes().getId(), ce);
  }

  private Properties getStreamsConfigForTest() {
    Properties props = getStreamsConfig();
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    /// Nooo! - need to configure deserialization class;/
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + brokerPort);


    return props;
  }

}
