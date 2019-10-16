package io.cloudevents.v03.kafka;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

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
public class CeHeaderBinarySerDes<T> implements Serde<T> {

  private final CeHeaderBinarySerializer<T> serializer;
  private final CeHeaderBinaryDeserializer<T> deserializer;
  private Class clazz;

//  public CeHeaderBinarySerializer(){};

  public CeHeaderBinarySerDes(Class clazz) {
    this.clazz = clazz;
    this.serializer = new CeHeaderBinarySerializer<T>();
    this.deserializer = new CeHeaderBinaryDeserializer<>(clazz);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public void close() {

  }

  @Override
  public Serializer<T> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return deserializer;
  }
}
