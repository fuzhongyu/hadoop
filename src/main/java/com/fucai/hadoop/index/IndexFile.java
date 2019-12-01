package com.fucai.hadoop.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * @author Fucai
 * @date 2019/7/1
 */

public class IndexFile implements Writable {

  private String fileName;

  private long wordCount;

  public IndexFile() {
  }

  public IndexFile(String fileName, long wordCount) {
    this.fileName = fileName;
    this.wordCount = wordCount;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public long getWordCount() {
    return wordCount;
  }

  public void setWordCount(long wordCount) {
    this.wordCount = wordCount;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(fileName);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    fileName = dataInput.readUTF();
    wordCount = dataInput.readLong();
  }

  @Override
  public String toString() {
    return " "+ fileName + "$" + wordCount;
  }
}
