package com.fucai.kafka_storm;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * @author Fucai
 * @date 2019/8/19
 */

public class KafkaStormSample {

  public static void main(String[] args) {
    TopologyBuilder topologyBuilder = new TopologyBuilder();

    // 设置kafka服务配置
    KafkaSpoutConfig<String,String> kafkaSpoutConfig =KafkaSpoutConfig.builder("master:9092,slave1:9092,slave2:9092", "kafka_storm_test")
        // 设置kafka消费组
        .setProp("group.id","kafka_storm_test_group")
        // 设置session超时时间
        .setProp(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"100000")
        // 设置拉取最大容量
        .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,"1048576")
        // 设置控制客户端等待请求响应的最大时间
        .setProp(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,"300000")
        // 设置心跳到消费者协调器之间的预期时间
        .setProp(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,"30000")
        // 设置offset提交时间
        .setOffsetCommitPeriodMs(15000)
        .build();


    Config config = new Config();
    // 定义集群分配多少个进程来执行topology
    config.setNumWorkers(3);
    config.setNumAckers(1);
    config.setDebug(true);

    topologyBuilder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig),200);
    topologyBuilder.setBolt("word-split", new SplitBolt()).shuffleGrouping("kafka-spout");
    topologyBuilder.setBolt("word-count", new CountBolt()).shuffleGrouping("word-split");

    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology("KafkaStormSample",config,topologyBuilder.createTopology());
    Utils.sleep(10000);
    localCluster.shutdown();


//    try {
//      StormSubmitter.submitTopology("KafkaStormSample",config,topologyBuilder.createTopology());
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

  }

}
