package com.fucai.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @author fucai
 * Date: 2019-08-02
 */
public class UpperBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String brand = tuple.getStringByField("brand");
    basicOutputCollector.emit(new Values(brand.toUpperCase()));

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("upBrand"));
  }
}
