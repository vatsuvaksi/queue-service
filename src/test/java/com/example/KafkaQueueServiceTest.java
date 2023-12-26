package com.example;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaQueueServiceTest {

    private QueueService queueService = new KakfkaQueueService();
    private static final Properties properties ;
    static{
        properties = new Properties();
        String propFileName = "kafkaconfig.properties";
        try (InputStream inStream = KakfkaQueueService.class.getClassLoader().getResourceAsStream(propFileName)) {
            properties.load(inStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void calltoKafkaProducer(){
        queueService.push(properties.getProperty("url.producer")
                , "{\"value\": \"hello_kafka\"}");

    }
    @Test
    public void calltoKafkaConsumer(){
        System.out.println(queueService.pull(properties.getProperty("url.consumer")).getBody());
    }
}
