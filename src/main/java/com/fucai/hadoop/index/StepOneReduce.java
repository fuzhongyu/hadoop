package com.fucai.hadoop.index;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Fucai
 * @date 2019/7/1
 */

public class StepOneReduce extends Reducer<Text, IndexFile, Text , IndexFile> {

  @Override
  protected void reduce(Text key, Iterable<IndexFile> values, Context context)
      throws IOException, InterruptedException {
    long counter = 0;
    for (IndexFile value:values){
      counter += value.getWordCount();
    }
  }

}
