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
public class CeBinarySerializerOLD<A  extends AttributesImpl, T> implements Serializer<CloudEvent<A, T>>, Deserializer<CloudEvent<A, T>> {

//  static class GenericCls<T>{
//    private Class<T> type;
//    public GenericCls(Class<T> cls){
//      type = cls;
//    }
//    Class<T> getType() {return type;}
//  }

  public ProducerRecord<String, T> getProducerRecordFromCloudEvent(String topic, CloudEvent<A, T> ce) {
    return new ProducerRecord<String, T>(topic, null, getRecordKey(ce), ce.getData().get(), getHeaders(ce));
  }
  private String getRecordKey(CloudEvent<A, T> ce) {
    return ce.getAttributes().getId();
  }

  private Iterable<Header> getHeaders(CloudEvent<A, T> ce) {
    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader("ce_id", ce.getAttributes().getId().getBytes()));
    headers.add(new RecordHeader("ce_type", ce.getAttributes().getType().getBytes()));
    headers.add(new RecordHeader("ce_mediatype", ce.getAttributes().getMediaType().orElse("none").getBytes()));
    headers.add(new RecordHeader("ce_specversion", ce.getAttributes().getSpecversion().getBytes()));
    headers.add(new RecordHeader("ce_source", ce.getAttributes().getSource().toString().getBytes()));
//        headers.add(new RecordHeader("ce_schemaUrl", cloudEvent.getAttributes().getSchemaurl().get().toString().getBytes()));
//        headers.add(new RecordHeader("ce_time", cloudEvent.getAttributes().getTime().get().toString().getBytes()));
    headers.add(new RecordHeader("ce_datacontenttype", ce.getAttributes().getDatacontenttype().orElse("none").getBytes()));
    headers.add(new RecordHeader("ce_datacontentenconding", ce.getAttributes().getDatacontentencoding().orElse("none").getBytes()));
    return headers;
  }




  @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

  @Override
  public CloudEvent<A, T> deserialize(String topic, byte[] data) {
    throw new NotImplementedException();
  }

  @Override
  public byte[] serialize(String topic, CloudEvent<A, T> cloudEvent) {
    throw new NotImplementedException();
  }

  @Override
  public byte[] serialize(String topic, Headers headers, CloudEvent<A, T> ce) {

        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.id, ce.getAttributes().getId().getBytes()));
        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.type, ce.getAttributes().getType().getBytes()));
//        if (ce.getAttributes().getMediaType().isPresent()) headers.add(new RecordHeader("ce_" + ContextAttributes."ce_mediatype", ce.getAttributes().getMediaType().get().getBytes()));
        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.specversion, ce.getAttributes().getSpecversion().getBytes()));
        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.source, ce.getAttributes().getSource().toString().getBytes()));
        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.schemaurl, ce.getAttributes().getSchemaurl().get().toString().getBytes()));
        // TODO: fix time formatting
        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.time, ce.getAttributes().getTime().get().toString().getBytes()));
        if (ce.getAttributes().getDatacontenttype().isPresent()) headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.datacontenttype, ce.getAttributes().getDatacontenttype().get().getBytes()));
        if (ce.getAttributes().getDatacontentencoding().isPresent()) headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.datacontentencoding, ce.getAttributes().getDatacontentencoding().orElse("none").getBytes()));

        // load serializer specified by the user
        // application avro - load avro serializer etc
      return Json.binaryEncode(ce.getData().get());
    }

    @Override
    public void close() {
    }

  @Override
  public CloudEvent<A, T> deserialize(String topic, Headers headers, byte[] data) {

    CloudEventBuilder<T> builder = CloudEventBuilder.builder();

    AttributesImpl attributes = AttributesImpl.unmarshal(AttributeMapper.map(headersToMap(headers)));
    Collection<ExtensionFormat> extensions = new ArrayList<>();
    CloudEvent<AttributesImpl, T> build = builder.build(Json.binaryDecodeValue(data, null), attributes, extensions);

//    return value;
    throw new RuntimeException("what!!");
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
