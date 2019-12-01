package com.fucai.storm;

import java.util.Map;
import java.util.Random;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * @author Fucai
 * @date 2019/8/2
 */

public class RandomWordSpout extends BaseRichSpout {

  private SpoutOutputCollector spoutOutputCollector;

  String[] words = {"iphone", "xiaomi", "mate", "sony", "sumsung", "moto", "meizu"};


  @Override
  public void open(Map map, TopologyContext topologyContext,
      SpoutOutputCollector spoutOutputCollector) {
    this.spoutOutputCollector = spoutOutputCollector;
  }

  /**
   * 初始化的时候会调用一次
   */


  @Override
  public void nextTuple() {
    Random random = new Random();
    int index = random.nextInt(words.length);
    String godName = words[index];
    spoutOutputCollector.emit(new Values(godName));
    Utils.sleep(5000);

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("brand"));
  }

}
