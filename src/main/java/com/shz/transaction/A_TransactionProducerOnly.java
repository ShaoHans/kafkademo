package com.shz.transaction;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * 生产者事务Only
 * 举例：某个topic有三个分区（0,1,2），当生产者同一时间分别给这三个分区发送不同的消息，如果发给1分区的消息失败，0分区和2分区的成功，由于开启了事务，则0和2分区的消息需要回滚。
 */
public class A_TransactionProducerOnly {
    public static void main(String[] args) {
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

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // 1.初始化事务
        producer.initTransactions();
        try {
            // 2.开启事务
            producer.beginTransaction();
            ProducerRecord<String, String> record;
            for (int i = 0; i < 10; i++) {
                if (i == 5) {
                    throw new Exception("服务器炸了");
                }
                record = new ProducerRecord<>("topic03", "key" + i, "value" + i);
                producer.send(record);
                producer.flush();
            }
            // 3.提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
            // 4.终止事务
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }
}
