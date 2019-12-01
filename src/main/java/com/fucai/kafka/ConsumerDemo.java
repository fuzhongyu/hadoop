package com.fucai.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author fucai
 * Date: 2019-08-07
 */
public class ConsumerDemo {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers","192.168.219.131:9092,192.168.219.132:9092,192.168.219.133:9092");
    //设置消费组
    properties.put("group.id","group1");
    // 自动提及哦啊offsets
    properties.put("enable.auto.commit","true");
    properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String,String> comsumer = new KafkaConsumer<String, String>(properties);
    comsumer.subscribe(Arrays.asList("test2"));
    while (true){
      ConsumerRecords<String, String> records = comsumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String,String> record : records){
        System.out.println(record.toString());
      }

    }


  }
}
