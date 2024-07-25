//package com.example;
//
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig; import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.slf4j. Logger;
//import org.slf4j.LoggerFactory;
//import java.util.Properties;
//
//
//public class SimpleProducer {
//
//	private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class); 
//	private final static String TOPIC_NAME = "test";
//	private final static String BOOTSTRAP_SERVERS = "3.137.41.97:9092";
//	
//	// 프로듀서 생성 - 데이터 전송 준비  
//	public static void main (String[] args) {
//
//		// 1. 컨피그 세팅 
//		Properties configs = new Properties();
//        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        // 2. 카프카 프로듀서 생성 
//        KafkaProducer<String, String> producer = new KafkaProducer<>(configs); 
//
//        String messageValue = "testMessage";
//        
//        // 3. 레코드 생성 
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
//   
//        // 메세지 키, 파티션 번호를 가진 데이터 전송 시 사용 
//        // int partitionNo = 0;
//        // String messageKey = "testMessageKey";
//        // ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME,partitionNo, messageKey, messageValue);
//        
//        producer.send(record);
//        
//        logger.info("{}", record);
//        
//        producer.flush(); // 프로듀서 내부 버퍼에 가지고 있던 레코드 배치를 브로커로 전송
//        producer.close();
//	}
//	
//}
package com.example;


