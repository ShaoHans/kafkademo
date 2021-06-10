package com.shz.dml;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaService implements AutoCloseable {

    public static String broker_servers = "node01:9092,node02:9092,node03:9092";
    KafkaAdminClient adminClient;

    public KafkaService(){
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker_servers);
        adminClient = (KafkaAdminClient) KafkaAdminClient.create(properties);
    }

    // 如果topic不存在就创建
    public TopicDescription createIfNotExist(String topicName) throws ExecutionException, InterruptedException {
        if(!existTopic(topicName)){
            createTopic(topicName);
        }
        return getTopicDescription(topicName);
    }

    // 获取topic详情
    public TopicDescription getTopicDescription(String topicName) throws ExecutionException, InterruptedException {
        List<String> topics = Arrays.asList(topicName);
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topics);
        Map<String, TopicDescription> topicMap = describeTopicsResult.all().get();
        return topicMap.getOrDefault(topicName,null);
    }

    // 创建topic
    public void createTopic(String topicName) throws ExecutionException, InterruptedException {
        // 创建topic
        NewTopic topic = new NewTopic(topicName, 3, (short) 2);
        // 此方法是异步创建
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(topic));
        // 同步等待
        createTopicsResult.all().get();
    }

    public Set<String> listTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult topics = adminClient.listTopics();
        return topics.names().get();
    }

    public boolean existTopic(String topicName) throws ExecutionException, InterruptedException {
        return listTopics().contains(topicName);
    }

    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        adminClient.deleteTopics(Arrays.asList(topicName)).all().get();
    }

    @Override
    public void close() throws Exception {
        if(adminClient != null){
            adminClient.close();
        }
    }
}
