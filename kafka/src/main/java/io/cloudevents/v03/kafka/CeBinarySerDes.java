package io.cloudevents.v03.kafka;


import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
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
public class CeBinarySerDes<A  extends AttributesImpl, T> implements Serde<CloudEvent<A, T>> {

  private final CeBinarySerializer<A, T> serializer;
  private final CeBinaryDeserializer<A, T> deserializer;
  private Class clazz;

  public CeBinarySerDes(Class clazz) {
    this.clazz = clazz;
    this.serializer = new CeBinarySerializer<>();
    this.deserializer = new CeBinaryDeserializer<>(clazz);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public void close() {
  }

  @Override
  public Serializer<CloudEvent<A, T>> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<CloudEvent<A, T>> deserializer() {
    return deserializer;
  }
}
