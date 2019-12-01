package com.fucai.hadoop.flow_group;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Fucai
 * @date 2019/7/1
 */

public class FlowSumReduce extends Reducer<Text, FlowBean, Text, FlowBean> {


  @Override
  protected void reduce(Text key, Iterable<FlowBean> values, Context context)
      throws IOException, InterruptedException {
    long upFlowCount = 0;
    long downFlowCount = 0;

    for (FlowBean unit : values) {
      upFlowCount += unit.getUpFlow();
      downFlowCount += unit.getDownFlow();
    }

    context.write(key, new FlowBean(key.toString(), upFlowCount, downFlowCount));
  }
}
