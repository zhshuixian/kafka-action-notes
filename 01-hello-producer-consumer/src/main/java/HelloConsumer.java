import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka 消费者的 Hello World
 *
 * @author xian
 */
@Slf4j
public class HelloConsumer {


  private static final String TOPIC = "hello-kafka";

  private static final String CONSUMER_GROUP_ID = "test-consumer-4153";

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.128.11:9092");
    // 此 Consumer 的消费者组
    props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
    // 是否自动提交 Offset
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // 偏移量提交的频率单位毫秒
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    // Broker 超过多少毫秒没有收到客户端的心跳包则认为其已经挂掉了
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    // auto.offset.reset
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // 客户端 key - value 的反序列化
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
    // 客户端订阅的 Topic，可以用集合的方式同时订阅多个，也可以使用正则匹配模式
    kafkaConsumer.subscribe(Collections.singletonList(TOPIC));

    int i = 100;
    // 先拉取一下是因为消费者初始化后，并没有直接去拉取集群的元数据信息，跟 Producer 一样
    kafkaConsumer.poll(Duration.ofSeconds(1));
    // 客户端手动设置从什么 Offset 开始拉取消息， 假设设置为 400 开始
    kafkaConsumer.seek(new TopicPartition(TOPIC, 0), 400);
    while (i > 0){

      // 如果在 1s 内没有获取到数据，则返回空
      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
      for (ConsumerRecord<String, String> record : records) {
        i--;
        log.info("拉取到消息, partition ={} offset={}, key={}, value={}", record.partition(), record.offset(), record.key(), record.value());
      }
    }

    i = 100;
    // 自动提交 Offset
    while (i > 0){
      // 如果在 1s 内没有获取到数据，则返回空
      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
      Set<TopicPartition> partitions = records.partitions();
      for (TopicPartition partition:partitions){
        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
        for (ConsumerRecord<String, String> record : partitionRecords) {
          i--;
          log.info("拉取到消息, partition ={} offset={}, key={}, value={}", record.partition(), record.offset(), record.key(), record.value());
        }
        long offset = partitionRecords.get(partitionRecords.size() - 1).offset();
        // 按照分区，自动提交 Offset 方式 2
        kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1)));
      }
      // 自动提交方式 1
      kafkaConsumer.commitSync();
    }

    // 如果想将某个 分区 的消费者 Offset 重置为 200
    // kafkaConsumer.commitSync(Collections.singletonMap(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(200)));

  }
}
