package com.shz.idempotence;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class IdempotenceProducer {
    public static void main(String[] args) {
        // 1.create producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置ack应答模式
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 设置retry重试策略
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // 重试3次
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1); //为了测试，应答超时时间设为1ms

        // 开启幂等
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);   // 默认值是false
        // 有一条消息发送失败就阻塞，直至发送成功才开始发送下一条消息
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,1);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> record;
        for (int i = 0; i < 10; i++) {
            record = new ProducerRecord<>("topic03", "idempotence" + i, "test idempotence" + i);
            producer.send(record);
            producer.flush();
        }

        producer.close();
    }
}
