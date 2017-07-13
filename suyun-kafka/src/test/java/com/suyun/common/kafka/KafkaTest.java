package com.suyun.common.kafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static org.springframework.test.util.AssertionErrors.assertTrue;

/**
 * Test
 *
 * Created by IT on 2017/3/29.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfig.class)
public class KafkaTest {

    @Autowired
    private KafkaTemplate<String, byte[]> template;

    @Autowired
    private Listener listener;

    public void sendByMessage() throws InterruptedException {
        template.send(MessageBuilder.withPayload("Hello").setHeader(KafkaHeaders.TOPIC, "test3").build());
        template.flush();
        assertTrue("Listener should receive messages.", listener.latch1.await(10, TimeUnit.SECONDS));
    }

    //@Test
    public void send() throws InterruptedException {
        String topic = "test";
        String value = "value:world";

        JsonSerializer<String> serializer = new JsonSerializer<>();
        template.send("test", serializer.serialize(topic,value));
        template.flush();
        assertTrue("Listener should receive messages.", listener.latch1.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void foo() {
        assertTrue("Foo", true);
    }

    @Test
    public void seirializerTest() {
        String topic = "topic";
        String data = "Hello";
        JsonSerializer<String> serializer = new JsonSerializer<>();
        byte[] serialize = serializer.serialize(topic, data);

        JsonDeserializer<String> deserializer = new JsonDeserializer<>(String.class);
        String ndata = deserializer.deserialize(topic, serialize);
        System.out.println(ndata);
        assertTrue("should equal", data.equals(ndata));
    }

}
