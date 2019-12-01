package com.fucai.hadoop.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Fucai
 * @date 2019/6/30
 */

public class WorlCountReduce extends Reducer<Text,LongWritable,Text,LongWritable> {

  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {

    long count=0;
    for (LongWritable value:values){
      count+=value.get();
    }

    context.write(key,new LongWritable(count));
  }
}
