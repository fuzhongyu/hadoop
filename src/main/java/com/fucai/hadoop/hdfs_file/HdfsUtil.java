package com.fucai.hadoop.hdfs_file;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * @author Fucai
 * @date 2019/6/28
 */

public class HdfsUtil {

  public static void main(String[] args) {
//    upload();
//    download();
//    mkdir();
//    rm();
    listFiles();
  }



  public static void upload(){
    Configuration conf = new Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);
      fs.copyFromLocalFile(new Path("file:/Users/fuzhongyu/Desktop/tmp/application.log"),new Path("hdfs://192.168.219.131:9000/user/root/application.log"));
    }catch (IOException e){
      e.printStackTrace();
    }

  }

  public static void download(){
    Configuration conf = new Configuration();

    try {
      FileSystem fs = FileSystem.get(conf);

      fs.copyToLocalFile(new Path("hdfs://192.168.219.131:9000/user/root/application.log"),new Path("file:/Users/fuzhongyu/Desktop/tmp/application.log"));

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public static void listFiles(){
    Configuration conf = new Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);

      RemoteIterator<LocatedFileStatus>  list =fs.listFiles(new Path("hdfs://ns1/"),true);
      while (list.hasNext()){
        System.out.println(list.next().getPath().getName());
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public static void mkdir(){

    Configuration conf = new Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);

      fs.mkdirs(new Path("hdfs://192.168.219.131:9000/test/tmp"));

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public static void rm(){
    Configuration conf = new Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);

      fs.delete(new Path("hdfs://192.168.219.131:9000/test/tmp"),true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
