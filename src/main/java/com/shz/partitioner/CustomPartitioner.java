package com.shz.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

/**
 * 自定义分区策略，kafka自带的分区策略类是：DefaultPartitioner
 */
public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if(keyBytes == null){
            // 如果没有指定key值，就统一往0号分区发送
            return 0;
        }else{
            int numPartitions = cluster.partitionsForTopic(topic).size();
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    @Override
    public void close() {
        System.out.println("callback close");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("callback configure");
    }
}
