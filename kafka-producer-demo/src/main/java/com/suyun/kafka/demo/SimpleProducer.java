package com.suyun.kafka.demo;

import com.suyun.common.kafka.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Created by IT on 2017/7/12.
 */
@Component
@Configuration
@ComponentScan({"com.suyun.common"})
public class SimpleProducer {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleProducer.class);

    private static final String TOPIC = "simple-test";

    private static ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private static String[] contents;

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    public static void main(String[] args) {
        contents = foreverYoung().split("\n");
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(SimpleProducer.class);
        SimpleProducer producer = applicationContext.getBean(SimpleProducer.class);
        producer.start();
    }

    private void start() {
        LOGGER.info("-----------------------------------------");
        LOGGER.info("    SimpleProducer application started.  ");
        LOGGER.info("-----------------------------------------");

        scheduler.scheduleAtFixedRate(() -> {
            doSend(contents[randInt(0, contents.length)]);
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    private void doSend(String data) {
        data = data + " \t(" + System.currentTimeMillis() + ")";
        byte[] bytes = JsonSerializer.serialize(data);
        kafkaTemplate.send(TOPIC, bytes);
        kafkaTemplate.flush();
        LOGGER.info("[S] " + data);
    }

    /**
     * Random int within [min, max)
     * @param min inclusive
     * @param max exclusive
     * @return
     */
    public static int randInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max);
    }

    public static String foreverYoung() {
        return "May God bless and keep you always,\n" +
                "愿上帝的庇护与你同在\n" +
                "May your wishes all come true,\n" +
                "愿你能够梦想成真\n" +
                "May you always do for others\n" +
                "愿你永远帮助别人\n" +
                "And let others do for you.\n" +
                "也接受别人的恩惠\n" +
                "May you build a ladder to the stars\n" +
                "愿你可以造一把采摘繁星的云梯\n" +
                "And climb on every rung,\n" +
                "稳妥沿它而上\n" +
                "May you stay forever young,\n" +
                "愿你永远年轻\n" +
                "May you grow up to be righteous,\n" +
                "愿你长大后正直无私\n" +
                "May you grow up to be true,\n" +
                "愿你懂事时真实善良\n" +
                "May you always know the truth\n" +
                "愿你永远了解真理的方向\n" +
                "And see the lights surrounding you.\n" +
                "所到之处都有高灯明照\n" +
                "May you always be courageous,\n" +
                "愿你永远勇敢无畏\n" +
                "Stand upright and be strong,\n" +
                "坚韧不拔，意志坚强\n" +
                "May you stay forever young,\n" +
                "愿你永远年轻\n" +
                "May your hands always be busy,\n" +
                "愿你总是忙碌充实\n" +
                "May your feet always be swift,\n" +
                "愿你脚步永远轻盈\n" +
                "May you have a strong foundation\n" +
                "愿你根基牢固\n" +
                "When the winds of changes shift.\n" +
                "在变故横生之时\n" +
                "May your heart always be joyful,\n" +
                "愿你的心总是充满快乐\n" +
                "May your song always be sung,\n" +
                "愿你的歌曲能够永远被人传唱\n" +
                "May you stay forever young,\n" +
                "愿你永远年轻";
    }
}
