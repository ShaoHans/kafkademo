package com.shz.acks;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AckProducer {
    public static void main(String[] args) {
        // 1.create producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置ack应答模式
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 设置retry重试策略，会导致消息在kafka集群中保存多份
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // 重试3次
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1); //为了测试，应答超时时间设为1ms

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> record = new ProducerRecord<>("topic03", "ack", "test ack");
        producer.flush();
        producer.send(record);

        producer.close();
    }
}
