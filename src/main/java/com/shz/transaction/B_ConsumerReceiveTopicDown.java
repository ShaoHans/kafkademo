package com.shz.transaction;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class B_ConsumerReceiveTopicDown {
    /*
     * 启动多个消费者组实例，由Kafka协调消费者与分区的对应关系，同组下的所有消费者均分收到的消息，并且组内有序
     * */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        // 设置消费者的事务隔离级别为read_committed(读提交)，默认值是read_uncommitted(读未提交)
        // read_committed:只能读取producer提交事务的消息
        // read_uncommitted:
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 订阅topics，由Kafka协调消费者与分区的对应关系。当组内的某个消费者宕机后，kafka会自动重新分配消费者与分区对应关系
        consumer.subscribe(Arrays.asList("topicDown"));

        // 遍历消息队列
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                records.forEach(r -> System.out.println("topic=" + r.topic() + ",partition=" + r.partition() + ",key=" + r.key() + ",value=" + r.value()));
            }
        }
    }
}
