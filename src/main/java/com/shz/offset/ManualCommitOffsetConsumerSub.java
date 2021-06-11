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
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            // 记录消费者分区偏移量信息
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            if (!records.isEmpty()) {
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
}
