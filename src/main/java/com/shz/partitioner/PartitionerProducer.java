package com.shz.partitioner;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class PartitionerProducer {
    public static void main(String[] args) {
        // 1.create producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置自定义分区策略
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,CustomPartitioner.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> record;
        for (int i = 0; i < 10; i++) {
            // 测试没有指定key值时，查看消息是否全部发送到了0号分区
            record = new ProducerRecord<>("topic03", "value" + i);

            // 测试指定了key值，查看结果是否根据hash运算发送到不同分区
            //record = new ProducerRecord<>("topic03", "key" + i, "value" + i);
            producer.send(record);
        }

        producer.close();
    }
}
