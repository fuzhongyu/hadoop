package com.fucai.hadoop.flow_partition;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Fucai
 * @date 2019/7/1
 */

public class AreaPartition extends Partitioner {

  public static final Map<String,Integer> AREA_MAP = new HashMap(){{
    put("15068831231",1);
    put("15068831234",2);
    put("15068831236",3);
  }};

  @Override
  public int getPartition(Object o, Object o2, int i) {
    Integer person = AREA_MAP.get(o.toString());
    return person==null?0:person;
  }
}
