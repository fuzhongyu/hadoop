package com.fucai.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author fucai Date: 2019-08-07
 */
public class ProducerDemo {

  public static void main(String[] args) {
    Properties properties = new Properties();
    //该地址是集群子集
    properties
        .put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
    // 0-不等待成功返回，1-等leader写成功返回，all（-1）-等leader和所有follower写成功返回
    properties.put("acks", "all");
    // 失败重试次数
    properties.put("retries", 3);
    // 每个分区位发送消息总字节大小（字节），超过设置的值就会提交数据到服务端
    properties.put("batch.size",10);
    // 数据在缓冲区保留对时间，超过设置对值就会被提交到服务端
    properties.put("linger.ms",10000);
    // Producer总体内存大小，要大于batch.size,小于物理内存， 否则会报申请内存不足
    properties.put("buffer.memory",10240);
    //设置事务id
    properties.put("transactional.id","first-transactional");
    // 设置幂等性，当设置事务的时候幂等性自动会设置成true
    properties.put("enable.idempotence",true);
    // key 序列化方式
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    // value序列化方式
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer<String, String> producer = new KafkaProducer(properties);
    // 初始化事务, 对于一个生产者，只能执行一次初始化事务操作
    producer.initTransactions();

    for (int i = 0; i < 1000; i++) {
      // 开启事务
      producer.beginTransaction();

      try {
        ProducerRecord<String, String> record = new ProducerRecord<>("kafka_storm_test", "key" + i, "value " + i);
        producer.send(record);

        ProducerRecord<String,String> record2 = new ProducerRecord<>("test2","key2"+i,"value2"+i);
        producer.send(record2);
        Thread.sleep(1000);

        // 提交事务
        producer.commitTransaction();
      } catch (Exception e){
        e.printStackTrace();
        // 放弃事务
        producer.abortTransaction();
      }

    }

    producer.close();

  }

}

