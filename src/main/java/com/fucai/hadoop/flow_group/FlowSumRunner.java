package com.fucai.hadoop.flow_group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class FlowSumRunner extends Configured implements Tool {

  @Override
  public int run(String[] strings) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    job.setJarByClass(FlowSumRunner.class);

    job.setMapperClass(FlowSumMapper.class);
    job.setReducerClass(FlowSumReduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowBean.class);

    FileInputFormat.setInputPaths(job, new Path("/user/root/flow_info"));
    FileOutputFormat.setOutputPath(job, new Path("/flow_bean_output"));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new FlowSumRunner(), args);
  }
}
