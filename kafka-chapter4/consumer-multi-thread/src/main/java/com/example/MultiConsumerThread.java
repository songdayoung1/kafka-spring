package com.example;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MultiConsumerThread {
    private final static Logger logger = LoggerFactory.getLogger(MultiConsumerThread.class);

    private final static String TOPIC_NAME = "test";
    private static String BOOTSTRAP_SERVERS = "3.137.41.97:9092";
    private final static String GROUP_ID = "test-group";
    private final static int CONSUMER_COUNT = 3;

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            ConsumerWorker worker = new ConsumerWorker(configs, TOPIC_NAME, i);
            executorService.execute(worker);
        }
    }
        
}
