package com.shz.serializer;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SerializerProducer {
    public static void main(String[] args) {
        // 1.create producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ObjectSerializer.class.getName());
        KafkaProducer<String, Student> producer = new KafkaProducer<>(props);

        ProducerRecord<String, Student> record;
        for (int i = 0; i < 10; i++) {
            record = new ProducerRecord<>("topic03", "key" + i, new Student(i, "shz" + i));
            producer.send(record);
        }

        producer.close();
    }
}
