package com.fucai.hadoop.index;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Fucai
 * @date 2019/7/1
 */

public class StepOneMapper extends Mapper<LongWritable, Text, Text, IndexFile> {

  @Override
  protected void map(LongWritable key,Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();

    String[] fields = line.split(" ");

    FileSplit inputSplit = (FileSplit) context.getInputSplit();

    String fileName = inputSplit.getPath().getName();

    for (String field:fields){
      context.write(new Text(field),new IndexFile(fileName,1));
    }


  }
}
