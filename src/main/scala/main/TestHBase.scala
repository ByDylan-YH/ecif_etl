package main

import java.io.IOException
import java.util

import dao.HBaseDao
import dao.impl.HBaseDaoImpl
import manager.HBaseJDBCManager
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import utils.BytesUtils

import scala.collection.JavaConverters

/**
 * Author:BYDylan
 * Date:2020/8/29
 * Description:
 */
object TestHBase {
  private val systemInformation: String = System.getProperties.getProperty("os.name");
  val hbaseConf: Configuration = HBaseConfiguration.create();
  hbaseConf.set("hbase.zookeeper.quorum", "192.168.1.201")
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("HAOOP_USER", "root")
  //  hbaseConf.set(TableInputFormat.INPUT_TABLE, "ecifdb20191201:PTY_SRC_CUST_ID")
  val connection: Connection = ConnectionFactory.createConnection(hbaseConf);

  def main(args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf().setAppName("ecif_etl_yh")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.testing.memory", "4294960000")
      .set("spark.cores.max", "4")
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.network.timeout", "30")
      .set("hbase,zookeeper.quorum", "192.168.1.201,192.168.1.202,192.168.1.203")
      .set("hbase.zookeeper.property.clientPort", "2181")
      .set("HAOOP_USER", "root")
    if (systemInformation.contains("Window")) {
      sparkConf.setMaster("local[*]");
    }
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
    val sc: SparkContext = sparkSession.sparkContext;
    //    val sc = new SparkContext(sparkConf);
    //    val table: Table = connection.getTable(TableName.valueOf("ecifdb20191201:PTY_SRC_CUST_ID"))
    //    val scan = new Scan;
    //    val rs: ResultScanner = table.getScanner(scan)
    //    //通过循环输出
    //    for (r <- JavaConverters.asScalaIterator(rs.iterator())) {
    //      val name: String = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("f")));
    //      println(name);
    //    }
    //    读取三要素信息
    //    val ptySrcCustIdDtf = sparkSession.createDataFrame(getPtySrcCustIdDtf("ecifdb20191201:PTY_SRC_CUST_ID", sc, "20191201"))
    //      .toDF("cust_id", "cust_type_cd", "src_cust_id", "src_sys");
    //    ptySrcCustIdDtf.show();
    val custIdList: util.List[String] = getColumnValueFilter("ecifdb20191201:PTY_SRC_CUST_ID", "f", "cust_id", "src_cust_id", "800113");
    val custId: String = custIdList.get(0);
    val certTypeCdList: util.List[String] = getColumnValueFilter("ecifdb20191201:PTY_CUST_FLG"
      , "f"
      , "cert_type_cd"
      , "cust_id"
      , custId);
    println(certTypeCdList);

    sc.stop();
  }

  def getColumnValueFilter(tableName: String, family: String, targetColumn: String, queryColumn: String, queryColumnValue: String): util.List[String] = {
    val returnList = new util.ArrayList[String];
    try {
      val table: Table = HBaseJDBCManager.getConnection.getTable(TableName.valueOf(tableName));
      val filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(queryColumn), CompareOperator.EQUAL, Bytes.toBytes(queryColumnValue));
      val scan = new Scan;
      scan.setFilter(filter);
      val resultScanner: ResultScanner = table.getScanner(scan);
      for (result <- JavaConverters.asScalaIterator(resultScanner.iterator())) {
        returnList.add(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(targetColumn))));
      }
    } catch {
      case e: IOException =>
        e.printStackTrace();
    }
    return returnList;
  }
}
