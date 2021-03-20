import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xian
 */

@Slf4j
public class HelloProducer {

    /**
     * 目标主题
     */
    private static final String TOPIC = "test";

    /**
     * 异步方式发送消息成功和失败的统计
     */
    public static AtomicLong asyncSend = new AtomicLong(0);
    public static AtomicLong asyncSendSuccess = new AtomicLong(0);
    public static AtomicLong asyncSendFailed = new AtomicLong(0);
    /**
     * 发送的消息条数
     */
    private static final int NUM = 100;

    public static final CountDownLatch LATCH = new CountDownLatch(NUM);

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        // Kafka 集群的地址，多个用 英文逗号 隔开 192.168.128.11:9092,192.168.128.12:9092
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.128.11:9092");
        // Key 和 Value 的系列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 客户端 id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "HelloKafka");
        // 重试次数和重试时间间隔
        props.put(ProducerConfig.RETRIES_CONFIG, "10");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "10");
        // ACKS
        props.put(ProducerConfig.ACKS_CONFIG, "0");

        // 初始化 Kafka Producer 的客户端， key 是 Integer， value 是 String 字符串
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 同步的方式发生 100 条 Hello Kafka 的消息
        for (int i = 0; i <= NUM; i++) {
            try {
                String messageKey = "sync-key-" + i;
                String messageValue = "sync-value-hello-kafka-" + i;
                long startTime = System.currentTimeMillis();
                // send() 其实也是异步的方式发送的，返回 Future，使用 get 会阻塞到结果返回
                // metadata 是一条消息发送成功后返回的元数据信息
                RecordMetadata metadata = producer.send(new ProducerRecord<>(TOPIC, messageKey, messageValue)).get();
                log.info("同步方式发送一条消息耗时{}ms, 第{}条消息, key={}, message={}，partition={}, offset={}",
                        System.currentTimeMillis() - startTime, i, messageKey, messageValue, metadata.partition(), metadata.offset());


            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        // 异步方式发送
        for (int i = 0; i < NUM; i++) {
            String messageKey = "async-key-" + i;
            String messageValue = "async-value-hello-kafka-" + i;
            producer.send(new ProducerRecord<>(TOPIC, messageKey, messageValue),
                    new SimpleCallback(messageKey, messageValue, System.currentTimeMillis()));

        }
        if(!LATCH.await(5, TimeUnit.MINUTES)) {
            log.error("超时了！");
        }


        log.info("异步方式发送{}条消息，成功{}条,失败{}条", asyncSend.get(), asyncSendSuccess.get(), asyncSendFailed.get());
        producer.close();
    }
}

@Slf4j
class SimpleCallback implements Callback {

    private final String key;
    private final String message;
    private final Long startTime;

    public SimpleCallback(String key, String message, long startTime) {
        this.key = key;
        this.message = message;
        this.startTime = startTime;
        HelloProducer.asyncSend.incrementAndGet();
    }

    /**
     * 异步发送的方式:实现一个 Callback,这个方法是消息发送成功或者失败后的回调
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if(metadata != null) {
            HelloProducer.asyncSendSuccess.incrementAndGet();
            log.info("异步发送一条消息耗时{}ms ,key={}, message={}，partition={}, offset={}"
                    , elapsedTime, key, message, metadata.partition(), metadata.offset());
        } else {
            HelloProducer.asyncSendFailed.incrementAndGet();
            exception.printStackTrace();
        }
        HelloProducer.LATCH.countDown();
    }
}
