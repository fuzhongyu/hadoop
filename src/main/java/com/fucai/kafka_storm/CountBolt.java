package com.fucai.kafka_storm;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/**
 * @author Fucai
 * @date 2019/8/19
 */

public class CountBolt extends BaseBasicBolt {

//  private FileWriter fileWriter;
  Map<String,Integer> counters;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    counters=new HashMap<>();
//    try {
//      fileWriter = new FileWriter("/Users/fuzhongyu/Desktop/"+UUID.randomUUID());
//      fileWriter = new FileWriter("/root/tmp_file/storm_data/result/" + UUID.randomUUID());
//    } catch (IOException e){
//      e.printStackTrace();
//    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String str = tuple.getStringByField("splitWord");
    if (!counters.containsKey(str)){
      counters.put(str,1);
    } else {
      Integer count = counters.get(str);
      counters.put(str,++count);
    }

    String outStr =  "";
    for (Map.Entry<String,Integer> entry:counters.entrySet()) {
      outStr += (entry.getKey() + "  "+ entry.getValue() + ";");
    }

    System.out.println(outStr);

//    try {
//      fileWriter.write(outStr);
//      fileWriter.write("\n");
//      fileWriter.flush();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("countWord"));
  }

//  @Override
//  public void cleanup() {
//    for (Map.Entry<String,Integer> entry:counters.entrySet()){
//      System.out.println(entry.getKey()+":" + entry.getValue());
//    }
//  }
}
