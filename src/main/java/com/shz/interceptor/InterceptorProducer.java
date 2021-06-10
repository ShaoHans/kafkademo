package com.shz.interceptor;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class InterceptorProducer {
    public static void main(String[] args) {
        // 1.create producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置自定义的拦截器
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,CustomProducerInterceptor.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> record;
        for (int i = 0; i < 10; i++) {
            record = new ProducerRecord<>("topic03", "key" + i, "value" + i);
            producer.send(record);
        }

        producer.close();
    }
}
