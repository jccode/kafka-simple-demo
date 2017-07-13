package com.suyun.common.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Service
public class Listener {

    final CountDownLatch latch1 = new CountDownLatch(1);

    @KafkaListener(id = "foo", topics = "test")
    public void listen1(byte[] foo) {
        System.out.println("----------------");
        System.out.println("Receive message");
        JsonDeserializer<String> deserializer = new JsonDeserializer<>(String.class);
        System.out.println(deserializer.deserialize("test", foo));
        this.latch1.countDown();
    }

}