package com.suyun.kafka.demo;

import com.suyun.common.kafka.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Created by IT on 2017/7/12.
 */
@Component
@Configuration
@ComponentScan({"com.suyun.common"})
public class SimpleConsumer {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);

    private static final String TOPIC = "simple-test";

    public static void main(String[] args) throws IOException {
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(SimpleConsumer.class);
        SimpleConsumer consumer = applicationContext.getBean(SimpleConsumer.class);
        consumer.start();
    }

    private void start() throws IOException {
        LOGGER.info("-----------------------------------------");
        LOGGER.info("    SimpleConsumer application started.  ");
        LOGGER.info("-----------------------------------------");

        System.in.read();
    }

    @KafkaListener(id = "simpleConsumer", topics = TOPIC)
    public void consume(byte[] data) {
        String content = JsonSerializer.deserialize(data, String.class);
        LOGGER.info(content);
    }
}
