import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka 消费者的 Hello World
 *
 * @author xian
 */
@Slf4j
public class HelloConsumer {

  private static final String TOPIC = "test";

  private static final String CONSUMER_GROUP_ID = "test-consumer-id-0";

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.128.11:9092");
    // 此 Consumer 的消费者组
    props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
    // 是否自动提交 Offset
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
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

    while (i > 0){
      // 每秒去拉取一次最新的消息
      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
      for (ConsumerRecord<String, String> record : records) {
        i--;
        log.info("拉取到消息, partition ={} offset={}, key={}, value={}", record.partition(), record.offset(), record.key(), record.value());
      }
    }


  }
}
