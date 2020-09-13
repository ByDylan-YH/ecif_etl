package main

import manager.HBaseJDBCManager
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import utils.BytesUtils

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

    val l: Long = singleIncreament("FR_SEQ", "FR", "f", "id", 1);
    println(l)

    sc.stop();
  }

  def getPtySrcCustIdDtf(table: String,
                         sparkContext: SparkContext,
                         etl_date: String): RDD[(String, String, String, String)] = {
    hbaseConf.set(TableInputFormat.INPUT_TABLE, table); // 源系统和ecif对照表

    // 从数据源获取数据
    val pty_src_cust_id: RDD[(ImmutableBytesWritable, Result)] = sparkContext.newAPIHadoopRDD(hbaseConf
      , classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable]
      , classOf[Result])

    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    pty_src_cust_id.map(r => (
      Bytes.toString(r._2.getValue(Bytes.toBytes("f"), Bytes.toBytes("cust_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("f"), Bytes.toBytes("cust_type_cd"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("f"), Bytes.toBytes("src_cust_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("f"), Bytes.toBytes("src_sys")))
    ));
  }

  def singleIncreament(tableName: String, rowKey: String, columnFamily: String, column: String, amount: Long): Long = {
    //    println(prefix + tableName + " : " + rowKey + " : " + columnFamily + " : " + column + " : " + amount);
    val table: Table = HBaseJDBCManager.getConnection.getTable(TableName.valueOf("ecifdb20191201:FR_SEQ"));
    val increment: Long = table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamily), Bytes.toBytes(column), amount);
    return increment;
  }
}
