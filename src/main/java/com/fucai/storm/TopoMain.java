package com.fucai.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author fucai
 * Date: 2019-08-02
 */
public class TopoMain {

  public static void main(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("randomspout", new RandomWordSpout(),4);

    builder.setBolt("upbolt",new UpperBolt(),4).shuffleGrouping("randomspout");

    builder.setBolt("suffixbolt",new SuffixBolt(),4).shuffleGrouping("upbolt");

    StormTopology topology=builder.createTopology();

    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(4);
    conf.setNumAckers(0);

    try {
      // 集群提交
//      StormSubmitter.submitTopology("changeTopo",conf,topology);
      // 本地提交
      LocalCluster localCluster = new LocalCluster();
      localCluster.submitTopology("changeTopo",conf,topology);
    } catch (Exception e) {
      e.printStackTrace();
    }


  }
}
