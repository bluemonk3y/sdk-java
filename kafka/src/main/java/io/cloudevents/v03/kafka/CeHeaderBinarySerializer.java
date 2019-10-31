package io.cloudevents.v03.kafka;


import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.CloudEventImpl;
import io.cloudevents.v03.ContextAttributes;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serializer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Map;
import java.util.Optional;

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
public class CeHeaderBinarySerializer<T> implements Serializer<T> {

//  public CeHeaderBinarySerializer(){};

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, T data) {
    throw new NotImplementedException();
  }

  @Override
  public byte[] serialize(String topic, Headers headers, Object data) {

    Object payload = data;

    // trying to send CloudEvent - map it to the Kafka wire format
    if (data instanceof CloudEvent) {
      payload = marshall(headers, (CloudEvent<AttributesImpl, T>) data);
    }

    assert headers.lastHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.id) != null;

    // TODO: load serializer specified by the user
    // application avro - load avro serializer etc
    return Json.binaryEncode(payload);
  }

  private Optional<T> marshall(Headers headers, CloudEvent<AttributesImpl, T> ce) {
    // TODO: rework properly
    AttributesImpl ceAttributes = ce.getAttributes();
    headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.id, ceAttributes.getId().getBytes()));
        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.type, ceAttributes.getType().getBytes()));
        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.specversion, ceAttributes.getSpecversion().getBytes()));
        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.source, ceAttributes.getSource().toString().getBytes()));
        if (ceAttributes.getSubject().isPresent()) headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.subject, ceAttributes.getSubject().get().getBytes()));
        // TODO: sort URI storage format
        if (ceAttributes.getSchemaurl().isPresent()) headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.schemaurl, ceAttributes.getSchemaurl().get().toString().getBytes()));
        // TODO: fix time formatting
        if (ceAttributes.getTime().isPresent()) headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.time, ceAttributes.getTime().get().toString().getBytes()));
        if (ceAttributes.getDatacontenttype().isPresent()) headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.datacontenttype, ceAttributes.getDatacontenttype().get().getBytes()));
        if (ceAttributes.getDatacontentencoding().isPresent()) headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.datacontentencoding, ceAttributes.getDatacontentencoding().orElse("none").getBytes()));

        return ce.getData();
  }

  @Override
  public void close() {
  }

  public ProducerRecord<String, CloudEvent<AttributesImpl, T>> getProducerRecordWithCloudEvent(String topic, CloudEventImpl<T> ce) {
    return new ProducerRecord<>(topic, ce.getAttributes().getId(), ce);
  }
}
