package com.example;

import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ConsumerRebalanceListener : 리밸런스 감지 인터페이스  
public class RebalanceListener implements ConsumerRebalanceListener {
	private final static Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

	// 리밸런스 시작 전 호출 매서드 
	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		logger.warn("Partitions are revoked : " + partitions.toString());
	}

	// 리밸런스 끝난 뒤 파티션 할당 완료 후 호출 매서드 
	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		logger.warn("Partitions are assigned : " + partitions.toString());
	}

}
