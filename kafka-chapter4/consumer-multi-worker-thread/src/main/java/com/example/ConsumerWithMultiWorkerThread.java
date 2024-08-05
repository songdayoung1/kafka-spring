package com.example;

import java.util.Properties;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class ConsumerWithMultiWorkerThread {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWithMultiWorkerThread.class);

    private final static String TOPIC_NAME = "test";
    private static String BOOTSTRAP_SERVERS = "3.137.41.97:9092";
    private final static String GROUP_ID = "test-group";
    
    
    public static void main(String[] args) {
        
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        
        // ExecutorService 서비스 실행
        ExecutorService executorService = Executors.newCachedThreadPool();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            
            for (ConsumerRecord<String, String> record : records) {
                ConsumerWorker worker = new ConsumerWorker(record.value());
                
                // execute() : 레코드 별 스레드를 사용하여 병렬처리 
                executorService.execute(worker);
            }
        }
        
    }
