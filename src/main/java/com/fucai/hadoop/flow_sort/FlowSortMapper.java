package com.fucai.hadoop.flow_sort;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Fucai
 * @date 2019/7/1
 */

public class FlowSortMapper extends Mapper<LongWritable ,Text,FlowBean,NullWritable> {

  @Override
  protected void map(LongWritable key, Text value,Context context)
      throws IOException, InterruptedException {

    String line = value.toString();

    String[] words = line.split(" ");
    String phoneNB = words[1];
    long upFlow = Long.valueOf(words[2]);
    long downFlow = Long.valueOf(words[3]);

    context.write(new FlowBean(phoneNB,upFlow,downFlow),NullWritable.get());

  }

}
