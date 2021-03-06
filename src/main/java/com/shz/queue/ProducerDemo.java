package com.shz.queue;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // 1.create producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> record;
        for (int i = 0; i < 10; i++) {
            // 使用默认的分区策略：DefaultPartitioner
            // 1.没有指定key值时，kafka按照轮询的方式发给不同的分区
            //record = new ProducerRecord<>("topic03", "value" + i);

            // 2.指定了key值，kafka对key值进行hash再取模算出结果，发送给特定分区
            record = new ProducerRecord<>("topic03", "key" + i, "value" + i);

            // 3.指定了partition值，kafka发送给指定的分区
            //record = new ProducerRecord<>("topic03", 1,"key" + i, "value" + i);
            producer.send(record);
        }

        producer.close();
    }
}
