package com.fucai.kafka_storm;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @author Fucai
 * @date 2019/8/19
 */
public class SplitBolt extends BaseBasicBolt {


  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String str = tuple.getString(4);
    String[] words = str.split(" ");
    for (String unit : words){
      unit = unit.trim();
      if (!unit.isEmpty()){
        unit = unit.toLowerCase();
        basicOutputCollector.emit(new Values(unit));
      }
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("splitWord"));
  }
}
