package server.impl

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import dao.HBaseDao
import dao.impl.HBaseDaoImpl
import entity.TaskEnvEntity
import manager.{HBaseJDBCManager, ProManager}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Delete, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import server.TaskServer
import utils.EncryptUtils

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{JavaConverters, mutable}

/**
 * Author:BYDylan
 * Date:2020/9/10
 * Description:
 */
class FixMultiCustIdTaskServerImpl(taskName: String,
                                   taskClass: String,
                                   taskOrder: String,
                                   sourceSys: String
                               ) extends TaskServer(taskName, taskClass, taskOrder, sourceSys) with Serializable {
  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def mappingHbase(table: String,
                   sparkContext: SparkContext): RDD[(String, String, String, String, String, String)] = {
    val hBaseConf: Configuration = HBaseJDBCManager.getConnection.getConfiguration;
    //    源系统和ecif对照表
    hBaseConf.set(TableInputFormat.INPUT_TABLE, table);
    // 从数据源获取数据
    val pty_src_cust_id = sparkContext.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]);
    // 将数据映射为表  也就是将 RDD转化为 cust_id,name,cert_type_cd,cert_no
    pty_src_cust_id.map(r => (
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("pbk_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cust_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("name"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cert_type_cd"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cert_no"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cust_type_cd")))
    ));
  }

  override def process(taskEntity: TaskEnvEntity): Unit = {
    val sparkSession: SparkSession = taskEntity.sparkSession;
    val sparkContext: SparkContext = sparkSession.sparkContext;

    val etlDate: String = taskEntity.etlDate;
    val codeBroadcast = taskEntity.codeBroadcast;
//    val srcTable = s"${taskEntity.hbaseNamespace}PTY_SRC_CUST_ID";
//    val custFlgTable = s"${taskEntity.hbaseNamespace}PTY_CUST_FLG";
//    val groupTable = s"${taskEntity.hbaseNamespace}PTY_CUST_GROUP";
    val srcTable = s"ecifdb20191201:PTY_SRC_CUST_ID";
    val custFlgTable = s"ecifdb20191201:PTY_CUST_FLG";
    val groupTable = s"ecifdb20191201:PTY_CUST_GROUP";
    val hBaseConf: Configuration = HBaseJDBCManager.getConnection.getConfiguration;
    logger.info("srcTable: {} , custFlgTable: {} , groupTable: {}", srcTable, custFlgTable, groupTable);
    //    三要素表
    hBaseConf.set(TableInputFormat.INPUT_TABLE, custFlgTable);
    val custFlgRdd: RDD[(ImmutableBytesWritable, Result)] = sparkContext
      .newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]);
    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    logger.info("三要素表: {}", sparkSession.createDataFrame(mappingHbase(custFlgTable, sparkContext))
      .toDF("pbk_id", "cust_id", "name", "cert_type_cd", "cert_no", "cust_type_cd")
      .show(300));

    //    按客户号group by
    val custFlgResults: RDD[(String, Iterable[Result])] = custFlgRdd.map(kv => {
      val result: Result = kv._2;
      val cust_id: String = Bytes.toString(result.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cust_id")));
      (cust_id, result)
    }).groupByKey();
    logger.info("三要素表,按客户号group by: {}", custFlgResults.foreach(println));

    //    groupId 表
    hBaseConf.set(TableInputFormat.INPUT_TABLE, groupTable);
    val custGroupRdd: RDD[(ImmutableBytesWritable, Result)] = sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]);
    logger.info("groupId 表: {}", custGroupRdd.foreach(print));

    //    按客户号group by
    val custGroupResults: RDD[(String, Iterable[Result])] = custGroupRdd.map(kv => {
      val result = kv._2;
      val cust_id: String = Bytes.toString(result.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cust_id")));
      (cust_id, result)
    }).groupByKey();
    logger.info("groupId 表,按客户号group by: {}", custGroupResults.foreach(print));

    //    源系统和ecif对照表
    hBaseConf.set(TableInputFormat.INPUT_TABLE, srcTable);
    val srcRdd: RDD[(ImmutableBytesWritable, Result)] = sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]);
    logger.info("源系统表: {}", srcTable.foreach(print));

    val srcCustIdRdd: RDD[(Result, String, String, String)] = srcRdd.map(kv => {
      val result = kv._2
      val cust_id: String = Bytes.toString(result.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cust_id")));
      val srcCustId: String = Bytes.toString(result.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("src_cust_id")));
      val src_sys: String = Bytes.toString(result.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("src_sys")));
      //      val rowKey = Bytes.toString(result.getRow);
      (result, cust_id, srcCustId, src_sys)
    });
    logger.info("源系统和ecif对照表: {}", srcCustIdRdd.foreach(println));

    //    按客户号group by (cust_id, results)
    val srcResults: RDD[(String, Iterable[Result])] = srcCustIdRdd.map(tup => (tup._2, tup._1)).groupByKey();
    logger.info("源系统和ecif对照表,按客户号group by: {}", srcResults.foreach(println));

    // 先把同一源系统客户号和源系统但是ecif客户号不同的客户先过滤出来 ( src_cust_id + src_sys, cust_id)
    val srcWithMutliCustIdRdd: RDD[(String, Iterable[String])] = srcCustIdRdd.map(tup => {
      (tup._3 + "#sep_src_sys#" + tup._4, tup._2)
    }).groupByKey().filter(kvs => kvs._2.size > 1);
    logger.info("先把同一源系统客户号和源系统但是ecif客户号不同的客户先过滤出来: {}", srcWithMutliCustIdRdd.foreach(println));

    // 把所有的cust_id 去重展平
    val mutliCusts: RDD[(String, Int)] = srcWithMutliCustIdRdd.flatMap(_._2).distinct().map(cust_id => {
      (cust_id, 1)
    });
    logger.info("把所有的cust_id 去重展平: {}", mutliCusts.foreach(println));

    //    ( cust_id, result)
    val custFlgMap: collection.Map[String, Iterable[Result]] = mutliCusts.join(custFlgResults).map(joinRes => {
      (joinRes._1, joinRes._2._2)
    }).collectAsMap();

    val custGroupMap: collection.Map[String, Iterable[Result]] = mutliCusts.join(custGroupResults).map(joinRes => {
      (joinRes._1, joinRes._2._2)
    }).collectAsMap();

    val custSrcMap: collection.Map[String, Iterable[Result]] = mutliCusts.join(srcResults).map(joinRes => {
      (joinRes._1, joinRes._2._2)
    }).collectAsMap();

    //    相同源系统和源系统客户号的所有cust_id
    val groupWithSrc: Array[Set[String]] = srcWithMutliCustIdRdd.values.map(_.toSet).collect();

    // 合并交集
    val newBuffer: ArrayBuffer[Set[String]] = MergeCollection(groupWithSrc);

    val hbaseDao: HBaseDao = new HBaseDaoImpl(etlDate);
    val srcCustDeletes = new ArrayBuffer[Delete]();
    val custFlgDeletes = new ArrayBuffer[Delete]();
    val groupCustDeletes = new ArrayBuffer[Delete]();
    val srcCustIdPuts = new ArrayBuffer[Put]();
    val custFlgPuts = new ArrayBuffer[Put]();
    val groupCustPuts = new ArrayBuffer[Put]();

    newBuffer.foreach(custRows => {
      implicit val minCustId: String = custRows.min;
      custRows.filter(_ != minCustId).foreach(inCorrectCustId => {
        val iterable: Iterable[Result] = Array().toIterable;
        // 更新
        custFlgMap.getOrElse(inCorrectCustId, iterable).update(custFlgDeletes, custFlgPuts, Array("name", "cert_no", "cert_type_cd"));
        custGroupMap.getOrElse(inCorrectCustId, iterable).update(groupCustDeletes, groupCustPuts, Array("src_cust_id", "group_id"));
        custSrcMap.getOrElse(inCorrectCustId, iterable).update(srcCustDeletes, srcCustIdPuts, Array("src_cust_id", "src_sys"));
      })
    })
    hbaseDao.delete(srcCustDeletes, srcTable);
    hbaseDao.savePuts(srcCustIdPuts, srcTable);
    hbaseDao.delete(custFlgDeletes, custFlgTable);
    hbaseDao.savePuts(custFlgPuts, custFlgTable);
    hbaseDao.delete(groupCustDeletes, groupTable);
    hbaseDao.savePuts(groupCustPuts, groupTable);
  }

  //  合并交集
  def MergeCollection(groupWithSrc: Array[Set[String]]): ArrayBuffer[Set[String]] = {
    // 把有交集的放到buffer里面
    // 初始化一个跟groupWithSrc一样大小的数组, 全为-1
    val array = List.fill[Int](groupWithSrc.length)(-1).toArray
    val map: mutable.HashMap[String, ArrayBuffer[Int]] = mutable.HashMap[String, ArrayBuffer[Int]]()
    for (i <- groupWithSrc.indices) {
      groupWithSrc(i).foreach(s => {
        if (map.contains(s)) { // 如果已经存在
          map(s).append(i) // 在原来的基础上增加现在的索引
        } else {
          map.put(s, ArrayBuffer(i))
        }
      })
    }
    //    排序
    val values: List[ArrayBuffer[Int]] = map.values.toList.sortWith((a, b) => a.mkString("") < b.mkString(""));

    values.foreach(v => {
      val indexs = v.sortWith((a, b) => a < b);
      indexs.foreach(i => {
        val arrayI = array(i)
        if (indexs.size == 1) {
          if (arrayI == -1) { // 如果index只有一个而且位置为-1则直接插入
            array(i) = i;
          }
        } else { // 如果index不止一个
          val indeValues: mutable.Seq[Int] = indexs.map(array(_)).filter(_ != -1)
          val min = if (indeValues.isEmpty) i else indeValues.min // 找到所有索引位置最小的值, 如果没有就当前值
          if (arrayI == -1) { //如果位置为-1 则直接插入
            array(i) = min
          } else { // 如果位置已经有值, 则比对原值是否比现在值小, 插入小的
            array(i) = if (arrayI < min) arrayI else {
              values.foreach(arr => { // 如果有改动, 则所以包含这个索引的都需要改动
                if (arr.contains(i)) {
                  arr.foreach(ei => array(ei) = min)
                }
              })
              min
            }
          }
        }
      })
    });

    val tmap: mutable.HashMap[Int, ListBuffer[Int]] = mutable.HashMap[Int, ListBuffer[Int]]();
    for (i <- array.indices) {
      val e: Int = array(i);
      if (tmap.contains(e)) {
        tmap(e).append(i);
      } else {
        tmap.put(e, ListBuffer(i));
      }
    }

    val newBuffer: ArrayBuffer[Set[String]] = ArrayBuffer[Set[String]]();
    tmap.values.foreach(v => {
      var set: Set[String] = Set()
      v.foreach(set ++= groupWithSrc(_));
      newBuffer.append(set);
    })
    newBuffer
  }

  //  对每个表的result进行更新, 装载删除和put
  implicit class UpdateResult(ite: Iterable[Result]) {
    def update(del: ArrayBuffer[Delete], put: ArrayBuffer[Put], rowkeyField: Array[String])(implicit minCustId: String): Unit = {
      ite.foreach(result => {
        del.append(new Delete(result.getRow));
        put.append(result.updPut(rowkeyField));
      })
    }
  }

  //  装载put
  implicit class ResultPro(result: Result) {
    def updPut(rowkeyField: Array[String])(implicit minCustId: String): Put = {
      val kvmap: mutable.Buffer[(String, String)] = mutable.Buffer[(String, String)]()
      for (x <- JavaConverters.asScalaIterator(result.listCells().iterator())) {
        val tuple: (String, String) = (Bytes.toString(CellUtil.cloneQualifier(x)), Bytes.toString(CellUtil.cloneValue(x)));
        kvmap.append(tuple);
      }
      val put = new Put(Bytes.toBytes(minCustId.reverse + EncryptUtils.md5Encrypt32(rowkeyField.map(kvmap.toMap.get(_) match { case None => ""; case Some(x) => x }).mkString(""))))
      kvmap.foreach(kv => {
        val fieldName: String = kv._1;
        val valueBytes: String = kv._2;
        if (fieldName == "cust_id") {
          put.addColumn(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes(fieldName), Bytes.toBytes(minCustId));
          put.addColumn(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("fixed_cust_id"), Bytes.toBytes(valueBytes));
        } else {
          put.addColumn(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes(fieldName), Bytes.toBytes(valueBytes));
        }
      })
      put.addColumn(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("data_upd_tm"), Bytes.toBytes(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
      return put;
    }
  }

}
