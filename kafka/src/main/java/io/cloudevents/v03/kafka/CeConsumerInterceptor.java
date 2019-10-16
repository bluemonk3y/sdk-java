package io.cloudevents.v03.kafka;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class CeConsumerInterceptor<K,V> implements ConsumerInterceptor<K,V> {
  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
    return null;
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
