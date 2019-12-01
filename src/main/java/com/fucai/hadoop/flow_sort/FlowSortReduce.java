package com.fucai.hadoop.flow_sort;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Fucai
 * @date 2019/7/1
 */

public class FlowSortReduce extends Reducer<FlowBean,NullWritable,FlowBean,NullWritable> {


  @Override
  protected void reduce(FlowBean key,Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {

    context.write(key,NullWritable.get());
  }
}
