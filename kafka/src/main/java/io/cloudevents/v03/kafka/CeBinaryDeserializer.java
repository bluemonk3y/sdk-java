package io.cloudevents.v03.kafka;


import io.cloudevents.CloudEvent;
import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.json.Json;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.CloudEventBuilder;
import io.cloudevents.v03.ContextAttributes;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

//import io.confluent.fstr.model.Payment;


/**
 * 2 modes:
 * [1] https://github.com/cloudevents/spec/blob/master/kafka-transport-binding.md#32-binary-content-mode
 * [2] https://github.com/cloudevents/spec/blob/master/kafka-transport-binding.md#33-structured-content-mode
 * @param <T>
 *
 *  Applied using:
 *  ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
 *
 * @author  @bluemonk3y
 */
public class CeBinaryDeserializer<A  extends AttributesImpl, T> implements Deserializer<CloudEvent<A, T>> {


  private final Class clazz;

  public CeBinaryDeserializer(Class clazz) {
    this.clazz = clazz;
  }

  @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

  @Override
  public CloudEvent<A, T> deserialize(String topic, byte[] data) {
    throw new NotImplementedException();
  }

  @Override
  public CloudEvent<A, T> deserialize(String topic, Headers headers, byte[] data) {

    System.out.println("DESER =========== HEADER PARSING ===========");
    StreamSupport.stream(headers.spliterator(), Boolean.FALSE).forEach(h -> System.out.println("H:" + h.key()));

    CloudEventBuilder<T> builder = CloudEventBuilder.builder();

    AttributesImpl attributes = AttributesImpl.unmarshal(AttributeMapper.map(headersToMap(headers)));
    Collection<ExtensionFormat> extensions = new ArrayList<>();

    CloudEvent<A, T> ce = (CloudEvent<A, T>) builder.build(Json.binaryDecodeValue(data, clazz), attributes, extensions);

    return ce;
  }

  /**
   * Turns Kafka headers into {@code Map<String, Object>}
   * @param kafkaHeaders
   * @return A {@link Map} with {@link Header} mapped as Object
   */
  private static Map<String, Object> headersToMap(Headers kafkaHeaders) {

    return
            StreamSupport.stream(kafkaHeaders.spliterator(), Boolean.FALSE)
                    .filter(header -> header.key().startsWith(AttributeMapper.HEADER_PREFIX))
                    .map(header ->
                            new AbstractMap.SimpleEntry<String, Object>(header.key(), header.value()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

}
