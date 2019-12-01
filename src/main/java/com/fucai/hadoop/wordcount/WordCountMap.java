package com.fucai.hadoop.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Fucai
 * @date 2019/6/30
 */

public class WordCountMap extends Mapper<LongWritable,Text,Text,LongWritable> {

  @Override
  protected void  map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {

    String line = value.toString();

    String[]  words = line.split(" ");

    for (String word : words){
      context.write(new Text(word),new LongWritable(1));
    }
  }

}
