package com.example;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleStreamApplication {
    private final static Logger logger = LoggerFactory.getLogger(SimpleStreamApplication.class);

    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVERS = "3.137.41.97:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "stream_log_copy";
    
    
    public static void main(String[] args) {
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(STREAM_LOG);

        stream.to(STREAM_LOG_COPY);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
    }
