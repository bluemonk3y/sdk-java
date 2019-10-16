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

/**
 * 2 modes:
 * [1] https://github.com/cloudevents/spec/blob/master/kafka-transport-binding.md#32-binary-content-mode
 * [2] https://github.com/cloudevents/spec/blob/master/kafka-transport-binding.md#33-structured-content-mode
 * @param <T>
 *
 *
 * @author  @bluemonk3y
 */
public class CeBinarySerializer<A  extends AttributesImpl, T> implements Serializer<CloudEvent<A, T>> {

  @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

  @Override
  public byte[] serialize(String topic, CloudEvent<A, T> cloudEvent) {
    throw new NotImplementedException();
  }

  @Override
  public byte[] serialize(String topic, Headers headers, CloudEvent<A, T> ce) {

    System.out.println("SERIALIZE =========== HEADER PARSING ===========");
    StreamSupport.stream(headers.spliterator(), Boolean.FALSE).forEach(h -> System.out.println("H:" + h.key() + " V:" + new String(h.value())));


        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.id, ce.getAttributes().getId().getBytes()));
        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.type, ce.getAttributes().getType().getBytes()));
//        if (ce.getAttributes().getMediaType().isPresent()) headers.add(new RecordHeader("ce_" + ContextAttributes."ce_mediatype", ce.getAttributes().getMediaType().get().getBytes()));
        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.specversion, ce.getAttributes().getSpecversion().getBytes()));
        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.source, ce.getAttributes().getSource().toString().getBytes()));
    headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.subject, ce.getAttributes().getSubject().get().getBytes()));
//        headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.schemaurl, ce.getAttributes().getSchemaurl().get().toString().getBytes()));
        // TODO: fix time formatting
        if (ce.getAttributes().getTime().isPresent()) headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.time, ce.getAttributes().getTime().get().toString().getBytes()));
        if (ce.getAttributes().getDatacontenttype().isPresent()) headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.datacontenttype, ce.getAttributes().getDatacontenttype().get().getBytes()));
        if (ce.getAttributes().getDatacontentencoding().isPresent()) headers.add(new RecordHeader(AttributeMapper.HEADER_PREFIX + ContextAttributes.datacontentencoding, ce.getAttributes().getDatacontentencoding().orElse("none").getBytes()));

        // load serializer specified by the user
        // application avro - load avro serializer etc
      return Json.binaryEncode(ce.getData().get());
    }

    @Override
    public void close() {
    }
}
