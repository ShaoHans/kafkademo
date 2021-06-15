package com.shz.offset;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class ManualCommitOffsetConsumerSub {
    /*
     * 启动多个消费者组实例，由Kafka协调消费者与分区的对应关系，同组下的所有消费者均分收到的消息，并且组内有序
     * */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g2");

        // 由消费者手动向kafka提交消费的偏移量
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);        //默认值：true

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅topics，由Kafka协调消费者与分区的对应关系。当组内的某个消费者宕机后，kafka会自动重新分配消费者与分区对应关系
        consumer.subscribe(Arrays.asList("topic03"));

        // 遍历消息队列
        while (true) {
            // 一次拉取多个分区的n多条消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(0));
            if (records.isEmpty()) {
                continue;
            }
            /**
             * 首先要明白一个问题：kafka是以分区的粒度来管理消费者的offset
             * 可以有以下三种方式提交offset
             * 1.按记录，每消费成功一条消息，就向kafka提交offset，准确率高，但性能不好（使用单线程）
             * 2.按分区，消费完分区内的所有消息后，再向kafka提交offset（可以单线程，也可以多线程）
             * 3.按批次，消费完所有分区的所有消息后，再向kafka提交offset（使用单线程）
             */
            // 记录消费者分区偏移量信息
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            records.forEach(r -> {
                offsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                System.out.println("topic=" + r.topic() + ",partition=" + r.partition() + ",key=" + r.key() + ",value=" + r.value());

                // 消费者向kafka服务器手动提交偏移量+1
                // 你可以注释或者反注释以下代码进行测试
                consumer.commitAsync(offsets, new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        System.out.println("offsets:" + offsets + "\t exception=" + exception);
                    }
                });
            });
        }
    }
}
