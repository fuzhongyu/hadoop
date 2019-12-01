package com.fucai.hadoop.flow_sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

public class FlowSortRunner extends Configured implements Tool {

  @Override
  public int run(String[] strings) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    job.setJarByClass(FlowSortRunner.class);

    job.setMapperClass(FlowSortMapper.class);
    job.setReducerClass(FlowSortReduce.class);

    job.setOutputKeyClass(FlowBean.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.setInputPaths(job,new Path("/user/root/flow_info"));
    FileOutputFormat.setOutputPath(job,new Path("/flow_bean_sort_output"));

    return job.waitForCompletion(true)?0:1;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new FlowSortRunner(),args);
  }
}
