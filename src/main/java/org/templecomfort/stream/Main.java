package org.templecomfort.stream;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.templecomfort.kafka.ComfortLoggerKafkaProducerCallback;
import org.templecomfort.kafka.KafkaClientCreator;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Scanner;

public class Main
{

  private static final String                         LOCATION_TO_LOG = "Users/kwas7493/Developer/karaf/apache/data/log/karaf.log";
  private              KafkaClientCreator             clientCreator;
  private              KafkaProducer<Integer, String> producer;
  private              KafkaConsumer<Integer, String> consumer;
  private              KafkaStreams                   streamer;

  private Main()
  {

  }

  private void init()
  {
    clientCreator = KafkaClientCreator.getInstance();
    producer = clientCreator.createKafkaProducer("localhost:9092");
    consumer = clientCreator.createKafkaConsumer("comfort-logger", "localhost:9092");
    streamer = clientCreator.createKafkaStreams("localhost:9092");
  }

  public static void main(String... args)
  {
    Main main = new Main();
    main.init();
  }

  private void logToTopic(String kafkaTopic) throws Exception
  {
    if (producer == null)
      throw new RuntimeException("Producer is not Initialized!!");
    HashMap<Integer, String> logMap = getLogsAsMap(LOCATION_TO_LOG);
    logMap.keySet().stream().forEach(eachKey -> {
      ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(kafkaTopic, eachKey, logMap.get(eachKey));
      producer.send(producerRecord, new ComfortLoggerKafkaProducerCallback());
    });
  }

  private HashMap<Integer, String> getLogsAsMap(String locationToLog) throws Exception
  {
    HashMap<Integer, String> map = new HashMap<>();
    int counter = 0;
    try (FileReader fileReader = new FileReader(new File(LOCATION_TO_LOG)))
    {
      Scanner scanner = new Scanner(fileReader);
      while (scanner.hasNextLine())
      {
        map.put(counter, scanner.nextLine());
        counter++;
      }
      scanner.close();
    }
    return map;
  }

}
