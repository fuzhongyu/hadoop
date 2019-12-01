package com.fucai.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/**
 * @author Fucai
 * @date 2019/7/14
 */

public class HbaseDao {
  private static Logger logger = LoggerFactory.getLogger(HbaseDao.class);

  private Configuration conf = null;
  private Connection connection = null;

  public HbaseDao() {
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum",
        "192.168.219.131:2181,192.168.219.132:2181,192.168.219.133:2181");
    try {
      this.connection = ConnectionFactory.createConnection(conf);
    } catch (IOException e) {
      e.printStackTrace();
      logger.error("gain connection fail");
    }
  }

  /**
   * 创建表
   * @param tableName 表名
   * @param columnFamily 字段名
   * @param splitKeys 分区key
   * @return
   */
  public boolean createTable(String tableName, List<String> columnFamily,byte[][] splitKeys) {
    Admin admin = null;
    try {
      admin = connection.getAdmin();
      List<ColumnFamilyDescriptor> familyDescriptorList = new ArrayList<>(columnFamily.size());
      columnFamily.forEach(unit -> {
        familyDescriptorList
            .add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(unit)).build());
      });

      TableDescriptor tableDescriptor = TableDescriptorBuilder
          .newBuilder(TableName.valueOf(tableName))
          .setColumnFamilies(familyDescriptorList)
          .build();

      if (admin.tableExists(TableName.valueOf(tableName))) {
        logger.info("table is arleady exist");
      } else {
        if (splitKeys!=null && splitKeys.length>0){
          admin.createTable(tableDescriptor,splitKeys);
        } else {
          admin.createTable(tableDescriptor);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      logger.error("create table fail");
      return false;
    } finally {
      close(admin, null, null);
    }
    return true;
  }

  /**
   * 自定义获取分区splitKeys
   * @param keys
   * @return
   */
  public byte[][] getSplitKeys(String[] keys){
    if (keys==null){
      keys = new String[]{"1|","2|","3|","4|","5|","6|","7|","8|","9|"};
    }
    byte[][] splitKeys = new byte[keys.length][];

    TreeSet<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (String unit:keys) {
      rows.add(Bytes.toBytes(unit));
    }

    Iterator<byte[]> rowKeyIter = rows.iterator();
    int i=0;
    while (rowKeyIter.hasNext()){
      byte[] tempRow = rowKeyIter.next();
      rowKeyIter.remove();
      splitKeys[i] = tempRow;
      i++;
    }
    return splitKeys;
  }

  /**
   * 按startKey 和endKey 分区数获取分区
   * @param startKey
   * @param endKey
   * @param numRegions
   * @return
   */
  public static byte[][] getHexSplites(String startKey,String endKey,int numRegions){
    byte[][] splits = new byte[numRegions-1][];
    BigInteger lowesKey = new BigInteger(startKey,16);
    BigInteger highestKey = new BigInteger(endKey,16);
    BigInteger range = highestKey.subtract(lowesKey);
    BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
    lowesKey = lowesKey.add(regionIncrement);
    for (int i=0;i<numRegions-1;i++){
      BigInteger key = lowesKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
      byte[] b = String.format("%016x",key).getBytes();
      splits[i] = b;
    }
    return splits;
  }

  /**
   * 获取表
   * @param tableName
   * @return
   */
  private Table getTable(String tableName){
    try {
      return connection.getTable(TableName.valueOf(tableName));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 获取所有表
   * @return
   */
  public List<String> getAllTableNames(){
    List<String> returnList = new ArrayList<>();
    Admin admin = null;
    try {
      admin = connection.getAdmin();
      TableName[] tableNames = admin.listTableNames();
      for (TableName unit:tableNames){
        returnList.add(unit.getNameAsString());
      }
    }catch (IOException e){
      e.printStackTrace();
      logger.error("get tables error");
    } finally {
      close(admin,null,null);
    }
    return returnList;
  }

  /**
   * 遍历查询指定表中所有数据
   * @param tableName
   * @return
   */
  public Map<String,Map<String,String>> getResultScanner(String tableName){
    Scan scan = new Scan();
    return this.queryData(tableName,scan);
  }

  /**
   * 根据startRowKey和stopRowKey遍历查询指定表中的所有数据
   * @param tableName
   * @param startRowKey
   * @param stopRowKey
   * @return
   */
  public Map<String,Map<String,String>> getResultScanner(String tableName,String startRowKey,String stopRowKey){
    Scan scan = new Scan();
    if (StringUtils.isNotBlank(startRowKey) && StringUtils.isNotBlank(stopRowKey)){
      scan.withStartRow(Bytes.toBytes(startRowKey));
      scan.withStopRow(Bytes.toBytes(stopRowKey));
    }
    return queryData(tableName,scan);
  }

  /**
   * 通过行前缀过滤器查询数据
   * @param tableName
   * @param prefix
   * @return
   */
  public Map<String,Map<String,String>> getResultScannerRowPrefixFilter(String tableName,String prefix){
    Scan scan = new Scan();
    if (StringUtils.isNotBlank(prefix)){
      scan.setRowPrefixFilter(Bytes.toBytes(prefix));
    }
    return queryData(tableName,scan);
  }

  /**
   * 通过列前缀过滤器查询数据
   * @param tableName
   * @param prefix
   * @return
   */
  public Map<String,Map<String,String>> getResultScannerColumnPrefixFilter(String tableName,String prefix){
    Scan scan = new Scan();
    if (StringUtils.isNotBlank(prefix)){
      Filter filter = new ColumnPrefixFilter(Bytes.toBytes(prefix));
      scan.setFilter(filter);
    }
    return queryData(tableName,scan);
  }

  /**
   * 行键模糊查询
   * @param tableName
   * @param keyWord
   * @return
   */
  public Map<String,Map<String,String>> getResultScannerRowFilter(String tableName,String keyWord){
    Scan scan = new Scan();
    if (StringUtils.isNotBlank(keyWord)){
      Filter filter = new RowFilter(CompareOperator.GREATER_OR_EQUAL,new SubstringComparator(keyWord));
      scan.setFilter(filter);
    }
    return queryData(tableName,scan);
  }

  /**
   * 查询列中包含特定字符的数据
   * @param tableName
   * @param keyWord
   * @return
   */
  public Map<String,Map<String,String>> getResultScannerColumnFilter(String tableName,String keyWord){
    Scan scan = new Scan();
    if (StringUtils.isNotBlank(keyWord)){
      Filter filter = new QualifierFilter(CompareOperator.GREATER_OR_EQUAL,new SubstringComparator(keyWord));
      scan.setFilter(filter);
    }
    return queryData(tableName,scan);
  }


  /**
   * 通过表名及过滤条件查询数据
   * @param tableName 表名
   * @param scan 过滤条件
   * @return
   */
  private Map<String,Map<String,String>> queryData(String tableName,Scan scan){
    Map<String,Map<String,String>> result = new HashMap<>();
    ResultScanner rs = null;
    Table table = null;
    try {
      table = getTable(tableName);
      rs = table.getScanner(scan);
      for (Result unit:rs){
        Map<String,String> columnMap = new HashMap<>();
        String rowKey = null;
        for (Cell cell:unit.listCells()){
          if (rowKey ==null){
            rowKey = Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
          }
          columnMap.put(Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength())
              ,Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
        }
        if (rowKey!=null){
          result.put(rowKey,columnMap);
        }
      }
    }catch (IOException e){
      e.printStackTrace();
      logger.error("get data fail");
    }finally {
      close(null,rs,table);
    }
    return result;
  }

  /**
   * 根据tableName和rowKey精确查询一行数据
   * @param tableName
   * @param rowKey
   * @return
   */
  public Map<String,String> getRowData(String tableName,String rowKey){
    Map<String,String> result = new HashMap<>();

    Get get = new Get(Bytes.toBytes(rowKey));

    Table table = null;
    try {
      table = getTable(tableName);
      Result hTableResult = table.get(get);
      if (hTableResult!=null && !hTableResult.isEmpty()){
        for (Cell unit:hTableResult.listCells()){
          result.put(Bytes.toString(unit.getQualifierArray(),unit.getQualifierOffset(),unit.getQualifierLength()),
              Bytes.toString(unit.getValueArray(),unit.getValueOffset(),unit.getValueLength()));
        }

      }
    } catch (IOException e){
      e.printStackTrace();
      logger.error("get data error");
    } finally {
      close(null,null,table);
    }

    return result;
  }

  /**
   * 根据tableName,rowKey ,familyName,column查询指定单元格数据
   * @param tableName 表名
   * @param rowKey 行键
   * @param familyName 列族名
   * @param columnName 列名
   * @return
   */
  public String getColumnValue(String tableName,String rowKey,String familyName,String columnName){
    String str = null;
    Get get = new Get(Bytes.toBytes(rowKey));
    Table table = null;
    try {
      table = getTable(tableName);
      Result result = table.get(get);
      if (result!=null && !result.isEmpty()){
        Cell cell = result.getColumnLatestCell(Bytes.toBytes(familyName),Bytes.toBytes(columnName));
        if (cell!=null){
          str = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
        }
      }
    } catch (IOException e){
      e.printStackTrace();
    } finally {
      close(null,null,table);
    }
    return str;
  }

  /**
   * 根据tableName,rowKey,familyName,column 查询指定单元格多个版本数据
   * @param tableName 表名
   * @param rowKey 行键
   * @param familyName 列族
   * @param columnName 列名
   * @param versions 版本数量
   * @return
   */
  public List<String> getColumnValuesByVersion(String tableName,String rowKey,String familyName,String columnName,int versions){
    List<String> result = new ArrayList<>(versions);
    Table table = null;
    try {
      table = getTable(tableName);
      Get get = new Get(Bytes.toBytes(rowKey));
      get.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columnName));
      get.readVersions(versions);

      Result hTableResult = table.get(get);
      if (hTableResult!=null && !hTableResult.isEmpty()){
        for (Cell unit:hTableResult.listCells()){
          result.add(Bytes.toString(unit.getValueArray(),unit.getValueOffset(),unit.getValueLength()));
        }
      }
    } catch (IOException e){
      e.printStackTrace();
    } finally {
      close(null,null,table);
    }
    return result;
  }

  /**
   * 更新数据
   * @param tableName
   * @param rowKey
   * @param familyName
   * @param columns
   * @param values
   */
  public void putData(String tableName,String rowKey,String familyName,String[] columns,String[] values){
    Table table = null;
    try {
      table = getTable(tableName);
      Put put = new Put(Bytes.toBytes(rowKey));
      if (columns!=null && values!=null && columns.length == values.length){
        for (int i=0;i<columns.length;i++){
          if (columns[i] !=null && values[i]!=null){
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columns[i]),Bytes.toBytes(values[i]));
          } else {
            throw new NullPointerException("列名和数据都不能为空");
          }
        }
      }
      table.put(put);
    } catch (Exception e){
      e.printStackTrace();
      logger.error("更新数据");
    } finally {
      close(null,null,table);
    }
  }

  /**
   * 表的某个字段赋值
   * @param tableName
   * @param rowKey
   * @param familyName
   * @param col
   * @param val
   */
  public void setColumnValue(String tableName,String rowKey,String familyName,String col,String val){
    Table table = null;
    try {
      table = getTable(tableName);
      Put put = new Put(Bytes.toBytes(rowKey));
      put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(col),Bytes.toBytes(val));
      table.put(put);
    } catch (IOException e){
      e.printStackTrace();
    } finally {
      close(null,null,table);
    }
  }

  /**
   * 删除字段数据
   * @param tableName
   * @param rowKey
   * @param familyName
   * @param columnName
   * @return
   */
  public boolean deleteColumn(String tableName,String rowKey,String familyName,String columnName){
    Table table = null;
    Admin admin = null;
    try {
      admin = connection.getAdmin();
      if (admin.tableExists(TableName.valueOf(tableName))){
        table = getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(familyName),Bytes.toBytes(columnName));
        table.delete(delete);
      }

    } catch (IOException e){
      e.printStackTrace();
      return false;
    } finally {
      close(admin,null,table);
    }
    return true;
  }

  /**
   * 删除行数据
   * @param tableName
   * @param rowKey
   * @return
   */
  public boolean deleteRow(String tableName,String rowKey){
    Table table = null;
    Admin admin = null;
    try {
      admin = connection.getAdmin();
      if (admin.tableExists(TableName.valueOf(tableName))){
        table = getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
      }
    }catch (IOException e){
      e.printStackTrace();
      return false;
    } finally {
      close(admin,null,table);
    }
    return true;
  }

  /**
   * 删除列族
   * @param tableName
   * @param columnFamily
   * @return
   */
  public boolean deleteColumnFamily(String tableName,String columnFamily){
    Admin admin = null;
    try {
      admin = connection.getAdmin();
      if (admin.tableExists(TableName.valueOf(tableName))){
        admin.deleteColumnFamily(TableName.valueOf(tableName),Bytes.toBytes(columnFamily));
      }

    } catch (IOException e){
      e.printStackTrace();
      return false;
    }finally {
      close(admin,null,null);
    }
    return true;
  }


  /**
   * 删除表
   * @param tableName
   * @return
   */
  public boolean deleteTable(String tableName){
    Admin admin =null;
    try {
      admin = connection.getAdmin();
      if (admin.tableExists(TableName.valueOf(tableName))){
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
      }
    } catch (IOException e){
      e.printStackTrace();
      return false;
    }
    return true;
  }


  /**
   * 关闭连接
   */
  private void close(Admin admin, ResultScanner rs, Table table) {
    if (admin != null) {
      try {
        admin.close();
      } catch (IOException e) {
        logger.error("close admin fail", e);
      }
    }

    if (rs != null) {
      rs.close();
    }

    if (table != null) {
      try {
        table.close();
      } catch (IOException e) {
        logger.error("close table fail", e);
      }
    }
  }

  public static void main(String[] args) {
    HbaseDao hbaseDao = new HbaseDao();
    List<String> tableNames = hbaseDao.getAllTableNames();
    System.out.println(Arrays.toString(tableNames.toArray()));
  }

}
