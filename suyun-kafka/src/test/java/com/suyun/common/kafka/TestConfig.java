package com.suyun.common.kafka;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Test config
 * Created by IT on 2017/3/29.
 */
@Configuration
@Import(KafkaConfig.class)
@ComponentScan("com.suyun.common.kafka")
public class TestConfig {

}
