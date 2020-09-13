package server.impl

import dao.TaskDao
import dao.impl.CMTaskDaoImpl
import entity.TaskEnvEntity
import manager.ParaManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import server.TaskServer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/9/6
 * Description:
 */
case class MultiFlgsTaskServerImpl(taskName: String,
                                   taskClass: String,
                                   taskOrder: String,
                                   createDataframeSql: String,
                                   implicit val sourceSys: String) extends TaskServer(taskName, taskClass, taskOrder, sourceSys) with Serializable {
  @transient val logger: Logger = LoggerFactory.getLogger(this.getClass);

  override def process(taskEnvEntity: TaskEnvEntity): Unit = {
    val etlDate: String = taskEnvEntity.etlDate;
    val cmTaskDao: TaskDao = new CMTaskDaoImpl(etlDate = etlDate);
    val sparkSession: SparkSession = taskEnvEntity.sparkSession;
    val sparkContext: SparkContext = sparkSession.sparkContext;
    val hbaseNamespace: String = taskEnvEntity.hbaseNamespace;
    val codeBroadcast: Broadcast[mutable.HashMap[String, String]] = taskEnvEntity.codeBroadcast;
    val groupIdBroadCast: Broadcast[mutable.HashMap[String, String]] = taskEnvEntity.groupIdBroadCast;
    val illegalCertsBroadcast: Broadcast[ArrayBuffer[String]] = taskEnvEntity.illegalCertsBroadcast;

    val sql: String = ParaManager.process(createDataframeSql, taskEnvEntity);
    logger.info("Executing SQL: {}\n", sql);

    implicit val codeMap = codeBroadcast.value;
    val resultDataFrame: DataFrame = sparkSession.sql(sql).explode("flgs", "flg") {
      flgs: String => {
        val nits: Array[String] = flgs.split("#ecif-flg-sep#", -1);
        // 如果证件类型需要转码, 则先转码.
        val nits1: Array[String] = nits.map(cmTaskDao.transform);
        // 如果都不合法就是临时客户, 临时客户存所有证件号不为空的记录, 如果证件号都为空随便存一条
        cmTaskDao.filterNull(nits1);
      }
    };
    logger.info("resultDataFrame: {}", resultDataFrame.show(300));

    val srcCustRdd: RDD[(String, String, String, String, String, String, String, String)] = resultDataFrame.rdd.mapPartitions(itr => {
      implicit val codeMap = codeBroadcast.value
      val groupMap = groupIdBroadCast.value
      itr.map(row => {
        val src_cust_id: String = row.getAs[String]("src_cust_id");
        val org_id: String = row.getAs[String]("org_id")
        val group_id: String = groupMap.getOrElse(org_id, "");
        val src_sys: String = row.getAs[String]("src_sys")
        val flg: String = row.getAs[String]("flg")
        val src_cust_type: String = row.getAs[String]("src_cust_type")
        val src_cust_status_bf: String = row.getAs[String]("src_cust_status_bf")
        val flgArr: Array[String] = flg.split("#ecif-sep#", -1);
        val name: String = flgArr(0).trim
        var src_cert_no: String = flgArr(1).trim
        var src_cert_type: String = flgArr(2).trim
        (src_cust_id, name, src_cert_no, src_cert_type, group_id, src_sys, src_cust_type, src_cust_status_bf)
      })
    });
    //    过滤客户名称和证件号码都为空的数据
    val filterRdd = srcCustRdd.filter(r => (StringUtils.isNotBlank(r._2) || StringUtils.isNotBlank(r._3)))

    val joinInfo: DataFrame = cmTaskDao.joinInfos(sparkSession, filterRdd, hbaseNamespace);
    val count: Long = joinInfo.count();
    val partitionNum: Int = (count / 10000 + 1).toInt;
    //    依据三要素group
    val illegalCerts: ArrayBuffer[String] = illegalCertsBroadcast.value;
    val kvRdd: RDD[(String, Iterable[(String, String, String, String, String, String, String, String, String)])] = cmTaskDao.groupByFlg(joinInfo, partitionNum, illegalCerts);
    logger.info("Result Count: {}", count);
    logger.info("Result partitions: {}", kvRdd.getNumPartitions);
    cmTaskDao.foreachFlg(kvRdd, hbaseNamespace, illegalCerts);
  }
}
