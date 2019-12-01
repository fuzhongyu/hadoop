package com.fucai.hadoop.flow_partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Fucai
 * @date 2019/7/1
 */

public class FlowBean implements Writable {

  private String phoneNB;
  private long upFlow;
  private long downFlow;
  private long sumFlow;

  public FlowBean() {
  }

  public FlowBean(String phoneNB, long upFlow, long downFlow) {
    this.phoneNB = phoneNB;
    this.upFlow = upFlow;
    this.downFlow = downFlow;
    this.sumFlow = upFlow + downFlow;
  }

  public String getPhoneNB() {
    return phoneNB;
  }

  public void setPhoneNB(String phoneNB) {
    this.phoneNB = phoneNB;
  }

  public long getUpFlow() {
    return upFlow;
  }

  public void setUpFlow(long upFlow) {
    this.upFlow = upFlow;
  }

  public long getDownFlow() {
    return downFlow;
  }

  public void setDownFlow(long downFlow) {
    this.downFlow = downFlow;
  }

  public long getSumFlow() {
    return sumFlow;
  }

  public void setSumFlow(long sumFlow) {
    this.sumFlow = sumFlow;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {

    dataOutput.writeUTF(phoneNB);
    dataOutput.writeLong(upFlow);
    dataOutput.writeLong(downFlow);
    dataOutput.writeLong(sumFlow);

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

    phoneNB = dataInput.readUTF();
    upFlow = dataInput.readLong();
    downFlow =dataInput.readLong();
    sumFlow = dataInput.readLong();
  }

  @Override
  public String toString(){
    return " " + phoneNB + "\t"+upFlow + "\t" + downFlow + "\t" + sumFlow;
  }

}
