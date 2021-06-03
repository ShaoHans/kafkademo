package com.shz;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class TopicDML {
    public static void main(String[] args) throws Exception {

        // 创建KafkaClient
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092,node04:9092");
        AdminClient adminClient = KafkaAdminClient.create(properties);

        // 创建topic
        NewTopic topic02 = new NewTopic("topic02", 3, (short) 2);
        // 此方法是异步创建
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(topic02));
        // 同步等待
        createTopicsResult.all().get();

        // 查看topic列表
        ListTopicsResult topics = adminClient.listTopics();
        Set<String> topicNames = topics.names().get();
        topicNames.forEach(n-> System.out.println(n));

        adminClient.close();

    }
}
