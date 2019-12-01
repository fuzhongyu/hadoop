package com.fucai.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Fucai
 * @date 2019/7/1
 */

public class WordCountJob {

  public static void main(String[] args) {
    Configuration conf = new Configuration();

    try {
      Job job =Job.getInstance(conf);

      //设置整个job的整个类jar包位置
      job.setJarByClass(WordCountJob.class);

      job.setMapperClass(WordCountMap.class);
      job.setReducerClass(WorlCountReduce.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);


      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(LongWritable.class);

      FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/user/root/hello"));

      FileOutputFormat.setOutputPath(job,new Path("hdfs://master:9000/output1"));

      // 提交任务
      job.waitForCompletion(true);


    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
