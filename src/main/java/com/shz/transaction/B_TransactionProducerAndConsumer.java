package com.shz.transaction;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * 消费者&生产者事务
 * 举例：当某一个应用实例既是消费者又是生产者的时候，如果它成功消费了上游topicUp的消息，但在发送给下游topicDown的消息失败后，它需要能回滚上游的消费操作，然后再次重试的时候不会因为重复消费上游消息导致数据不一致
 */
public class B_TransactionProducerAndConsumer {
    public static void main(String[] args) {
        String groupId = "g1";
        KafkaProducer<String, String> producer = buildProducer();
        KafkaConsumer<String, String> consumer = buildConsumer(groupId);

        // 1.初始化事务
        producer.initTransactions();
        consumer.subscribe(Arrays.asList("topicUp"));
        // 遍历消息队列
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            if (records.isEmpty()) {
                continue;
            }

            try {
                // 开启事务
                producer.beginTransaction();
                // 记录消费者分区偏移量信息
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                records.forEach(r -> {
                    offsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                    System.out.println("topic=" + r.topic() + ",partition=" + r.partition() + ",key=" + r.key() + ",value=" + r.value());

                    // 向下游topic发送消息
                    ProducerRecord<String, String> record = new ProducerRecord<>("topicDown", r.key(), r.value() + " downstream");
                    producer.send(record);
                    producer.flush();

//                    // 测试抛异常，回滚事务
//                    if(r.value().contains("8")){
//                        int i=10/0;
//                    }
                });
//                int i=10/0;

                // 提交消费者的偏移量
                producer.sendOffsetsToTransaction(offsets, groupId);
                // 提交事务
                producer.commitTransaction();
            } catch (Exception e) {
                e.printStackTrace();
                producer.abortTransaction();
            }
        }
    }

    public static KafkaProducer<String, String> buildProducer() {
        // 1.create producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 配置事务Id
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tid-" + UUID.randomUUID().toString());

        // 配置缓冲区大小（默认值：16384），缓冲区满了就会把所有消息发送到broker
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1000);
        // 发送消息的延迟时间（默认值为0，单位是毫秒，缓冲区满了后立即发送缓冲区中的消息），当时间达到指定值后，生产者会把缓冲区中所有的消息（即使没有达到缓冲区的size）发送给broker
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        // 设置acks和幂等性
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new KafkaProducer<>(props);
    }

    public static KafkaConsumer<String, String> buildConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 设置消费者的事务隔离级别为read_committed(读提交)，默认值是read_uncommitted(读未提交)
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // 必须关闭消费者的自动提交offset配置，改为手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);        //默认值：true

        return new KafkaConsumer<>(props);
    }
}
