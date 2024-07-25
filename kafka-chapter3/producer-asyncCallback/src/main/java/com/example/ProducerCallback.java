package com.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallback implements Callback {

	private final static Logger logger = LoggerFactory.getLogger(ProducerCallback.class);
	
	// 레코드의 비동기 결과를 받는 함수 
	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if(exception != null)
			// 브로거 적재 이슈가 생길 시 확인 가능  
			logger.error(exception.getMessage());
		else
			// 레코드에 적대된 데이터 확인 가능 
			logger.info(metadata.toString());
	}

}
