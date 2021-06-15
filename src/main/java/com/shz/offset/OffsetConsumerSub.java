package com.shz.offset;

import com.shz.dml.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

public class OffsetConsumerSub {
    /*
     * 启动多个消费者组实例，由Kafka协调消费者与分区的对应关系，同组下的所有消费者均分收到的消息，并且组内有序
     * */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaService.broker_servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        // 新加入的消费者要如何消费分区里面的消息？
        // 有三种方式：earliest，latest，none. 这三种方式仅仅针对分区里面没有消费者信息的时候才会有区分，当分区里面已经包含了消费者的信息时就没有区别了。
        // earliest：如果分区没有该消费者的偏移量信息，则读取该分区最早的偏移量；否则就取上一次读取的偏移量
        // latest：如果分区没有该消费者的偏移量信息，则读取该分区最新的偏移量；否则就取上一次读取的偏移量
        // none：报异常
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 消费者在消费数据的时候默认会定期向kafka服务器提交消费的偏移量，这样就可以保证所有的消息至少可以被消费者消费一次
        // 可以通过配置来设置定期的参数
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);        //默认值：true
        // 定期时间设为10秒，如果10秒内，消费者宕机，没来得及向kafka提交已经消费的偏移量，则下次启动的时候又从 AUTO_OFFSET_RESET_CONFIG 设置的参数值开始消费，导致重复消费
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);   //默认值：5000

        /**
         * 自动提交隐含的问题：由于是异步定期提交消费的offset，
         * 1.当成功消费完了消息之后提交offset之前，应用挂掉重启后会导致重新拉取旧消息进而导致重复消费
         * 2.当前批次的消息数据还没写入数据库，而offset已经提交了，恰好此时提交数据库事务发生异常或者consumer挂了，重启consumer，获取到了提交的offset，导致消息数据丢失
         */

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // 订阅topics，由Kafka协调消费者与分区的对应关系。当组内的某个消费者宕机后，kafka会自动重新分配消费者与分区对应关系
        consumer.subscribe(Pattern.compile("^topic.*"), new ConsumerRebalanceListener() {
            // consumer负载均衡策略
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("----onPartitionsRevoked----");
                partitions.forEach(p-> System.out.println(p.partition()));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 重新分配partition
                System.out.println("----onPartitionsAssigned----");
                partitions.forEach(p-> System.out.println(p.partition()));
            }
        });

        // 遍历消息队列
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                // 每次poll的时候是从多个分区同时拉取
                Set<TopicPartition> partitions = records.partitions();
                records.forEach(r -> {
                    System.out.println("topic=" + r.topic() + ",partition=" + r.partition() + ",key=" + r.key() + ",value=" + r.value());
                });
            }
        }
    }
}
