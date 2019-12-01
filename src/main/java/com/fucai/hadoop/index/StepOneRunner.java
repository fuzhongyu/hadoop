package com.fucai.hadoop.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Fucai
 * @date 2019/7/1
 */

public class StepOneRunner  extends Configured implements Tool {

  @Override
  public int run(String[] strings) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf);
    job.setJarByClass(StepOneRunner.class);

    job.setMapperClass(StepOneMapper.class);
    job.setReducerClass(StepOneReduce.class);


    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    FileInputFormat.setInputPaths(job,new Path("/index_file/"));
    FileOutputFormat.setOutputPath(job,new Path("/output_index"));

    return job.waitForCompletion(true)?0:1;
  }


  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new StepOneRunner(),args);
  }
}
