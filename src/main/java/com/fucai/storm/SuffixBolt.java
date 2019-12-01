package com.fucai.storm;

import java.util.UUID;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @author fucai
 * Date: 2019-08-02
 */
public class SuffixBolt extends BaseBasicBolt {

  private FileWriter fileWriter;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
//    try {
//      fileWriter = new FileWriter("/root/tmp_file/storm_data/result/"+UUID.randomUUID());
//    } catch (IOException e){
//      e.printStackTrace();
//    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:MM:ss");
    String upBrand = tuple.getStringByField("upBrand");
    String outStr = upBrand+ "   "+ format.format(new Date());
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

  }
}
