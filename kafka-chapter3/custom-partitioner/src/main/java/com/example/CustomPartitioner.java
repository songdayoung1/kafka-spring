package com.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

// 사용자 정의 파티션을 정의해 원하는 파티션에 데이터 적재 가능 
public class CustomPartitioner implements Partitioner {


    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCustomPartitioner.class);
    
	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		
		if (keyBytes == null) {
			// 1. 메시지 키 지정안했을 경우 
            throw new InvalidRecordException("Need message key");
        }
        if (((String)key).equals("Pangyo")) {
        	// 2. 메시지 키 지정할 경우
        	// 반환값 : 레코드가 들어갈 파티션 번호
        	logger.info("Key vaild check");    
            return 0; 
        }
        
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
	}
	
	

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
