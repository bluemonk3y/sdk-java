package io.cloudevents.v03.kafka;


import io.cloudevents.CloudEvent;
import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.json.Json;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.CloudEventBuilder;
import io.cloudevents.v03.CloudEventImpl;
import io.cloudevents.v03.ContextAttributes;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

//import io.confluent.fstr.model.Payment;


/**
 * 2 modes:
 * [1] https://github.com/cloudevents/spec/blob/master/kafka-transport-binding.md#32-binary-content-mode
 * [2] https://github.com/cloudevents/spec/blob/master/kafka-transport-binding.md#33-structured-content-mode
 *
 * @param <T> Applied using:
 *            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
 * @author @bluemonk3y
 */
public class CeHeaderBinaryDeserializer<T> implements Deserializer<T> {

  private Class clazz;

//  public CeHeaderBinarySerializer(){};

  public CeHeaderBinaryDeserializer(Class clazz) {
    this.clazz = clazz;
  }


  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    throw new NotImplementedException();
  }

  @Override
  public void close() {
  }

  @Override
  public T deserialize(String topic, Headers headers, byte[] data) {
    return Json.binaryDecodeValue(data, clazz);
  }

  public ProducerRecord<String, String> getProducerRecordFromCloudEvent(String topic, CloudEventImpl<T> ce) {

    return null;
  }

  public ProducerRecord<String, CloudEvent<AttributesImpl, T>> getProducerRecordWithCloudEvent(String topic, CloudEventImpl<T> ce) {
    return new ProducerRecord<>(topic, ce.getAttributes().getId(), ce);
  }
}
