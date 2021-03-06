package com.shz.queue;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class ConsumerAssignDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // 订阅topics，手动指定topic的分区
        TopicPartition partition = new TopicPartition("topic03", 0);
        List<TopicPartition> partitions = Collections.singletonList(partition);
        consumer.assign(partitions);
        // 从第一条消息读起
        //consumer.seekToBeginning(partitions);
        // 从偏移量为13的消息读起
        consumer.seek(partition,13);

        // 遍历消息队列
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                records.forEach(r -> System.out.println("topic=" + r.topic() + ",partition=" + r.partition() + ",key=" + r.key() + ",value=" + r.value()));
            }
        }
    }
}
