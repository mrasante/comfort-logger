package org.templecomfort.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ComfortLoggerKafkaProducerCallback implements Callback
{
  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception)
  {

  }
}
