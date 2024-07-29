package com.example;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWithRebalanceListener {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWithRebalanceListener.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "3.137.41.97:9092";
    private final static String GROUP_ID = "test-group"; // 그룹 지정
    private final static int PARTITION_NUMBER = 0; // 파티션 넘버 

    private static KafkaConsumer<String, String> consumer;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
    
    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());
        
        // 1. 컨슈머에 어떤 토픽, 파티션을 할당할지 명식적으로 표시하려면 아래와 같이 사용 가능 
        // consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));
        
        // 2. 컨슈머에 할당된 토픽과 파티션의 정보 확인을 위한 매서드 
        // Set<TopicPartition> assginedTopicPartition = consumer.assignment();
        
        
        while (true) {
        	ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        	for (ConsumerRecord<String, String> record : records) {
        		logger.info("{}", record);
        		currentOffset.put(new TopicPartition(record.topic(), record.partition()), 
        				new OffsetAndMetadata(record.offset() + 1, null));
        		
        		consumer.commitSync(currentOffset);
        	}
        }
    }
    
 // ConsumerRebalanceListener : 리밸런스 감지 인터페이스  
    public static class RebalanceListener implements ConsumerRebalanceListener {
    	private final static Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    	// 리밸런스 시작 전 호출 매서드 
    	@Override
    	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    		logger.warn("Partitions are revoked : " + partitions.toString());
    		consumer.commitSync(currentOffset);
    	}

    	// 리밸런스 끝난 뒤 파티션 할당 완료 후 호출 매서드 
    	@Override
    	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    		logger.warn("Partitions are assigned : " + partitions.toString());
    	}

    }
}