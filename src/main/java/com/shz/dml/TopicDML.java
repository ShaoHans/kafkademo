package com.shz.dml;

import org.apache.kafka.clients.admin.*;

import java.util.Set;

public class TopicDML {
    public static void main(String[] args) throws Exception {

        KafkaService kafkaService = new KafkaService();
        //kafkaService.deleteTopic("topic01");
        TopicDescription td = kafkaService.createIfNotExist("topicDown");
        System.out.println(td);

        Set<String> topics = kafkaService.listTopics();
        topics.forEach(t-> System.out.println(t));

    }
}
