package org.templecomfort.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import scala.collection.immutable.Stream;

import java.util.Properties;

/**
 * A singleton class to create kafka clients for whatever kafka cluster
 * the logger needs to access;
 */
public class KafkaClientCreator
{

  private static final String PRODUCER_CLIENT_ID = "Comfort-Logger-Producer-Client";
  private static final String COMFORT_STREAM_APP_ID = "Comfort-Logger-Stream-Client";
  private static KafkaClientCreator kafkaClientCreator;

  private KafkaClientCreator(){

  }

  private Properties createKafkaProducerProperties(String... bootstrapServers)
  {
    Properties properties = new Properties();
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getCanonicalName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);

    return properties;
  }

  private Properties createKafkaConsumerProperties(String consumerGroupId, String... bootstrapServers)
  {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getCanonicalName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put("group.id", consumerGroupId);
    return properties;
  }

  private Properties createKafkaStreamsProperties(String clusterBrokers){
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, clusterBrokers);
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, COMFORT_STREAM_APP_ID);
    return properties;
  }


  public KafkaConsumer<Integer, String> createKafkaConsumer(String consumerGroupId, String bootstrapServers){
    Properties props = createKafkaConsumerProperties(consumerGroupId, bootstrapServers);
    return  new KafkaConsumer<Integer, String>(props);
  }


  public KafkaProducer<Integer, String> createKafkaProducer(String servers){
    Properties props = createKafkaProducerProperties(servers);
    return  new KafkaProducer<Integer, String>(props);
  }

  public KafkaStreams createKafkaStreams(String clusterServers){
    Properties properties = createKafkaStreamsProperties(clusterServers);
    StreamsBuilder builder = new StreamsBuilder();
    Topology topology = builder.build();
    return new KafkaStreams(topology, properties);
  }

  public static KafkaClientCreator getInstance(){
    if(kafkaClientCreator == null)
      kafkaClientCreator = new KafkaClientCreator();
    else
      return kafkaClientCreator;
    return kafkaClientCreator;
  }

}
