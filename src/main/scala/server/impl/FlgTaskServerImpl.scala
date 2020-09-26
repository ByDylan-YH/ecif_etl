package server.impl

import dao.TaskDao
import dao.impl.CMTaskDaoImpl
import entity.TaskEnvEntity
import manager.ParaManager
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import server.TaskServer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description:
 */
case class FlgTaskServerImpl(taskName: String,
                             taskClass: String,
                             taskOrder: String,
                             createDataframeSql: String,
                             implicit val codeTrans: Map[String, String],
                             implicit val sourceSys: String) extends TaskServer(taskName, taskClass, taskOrder, sourceSys) with Serializable {

  override def process(taskEntity: TaskEnvEntity): Unit = {
    @transient val logger: Logger = LoggerFactory.getLogger(this.getClass);
    val sparkSession: SparkSession = taskEntity.sparkSession
    val etlDate: String = taskEntity.etlDate;
    val cmTaskDao: TaskDao = new CMTaskDaoImpl(etlDate);
    val codeBroadcast: Broadcast[mutable.HashMap[String, String]] = taskEntity.codeBroadcast;
    val hbaseNamespace: String = "ecifdb20191201:";
//    val hbaseNamespace: String = taskEntity.hbaseNamespace;
    val groupIdBroadCast: Broadcast[mutable.HashMap[String, String]] = taskEntity.groupIdBroadCast;
    val illegalCertsBroadcast: Broadcast[ArrayBuffer[String]] = taskEntity.illegalCertsBroadcast;
    //读取源系统客户信息
    val sql: String = ParaManager.process(createDataframeSql, taskEntity);
    logger.info("Executing SQL: {}\n", sql);
    //    此部分操作是将源系统数据转码并将org_id转为group_id
    //    在使用spark1.6的时候,创建SQLContext读取一个文件之后,返回DataFrame类型的变量可以直接.map操作,不会报错.但是升级之后会包一个错误
    //    方式二: 添加隐式转换
    //    implicit val demo= org.apache.spark.sql.Encoders.kryo[((String,String,String,String,String,String,String,String))];
    //    方式一: 添加 .rdd
    val srcCustRdd: RDD[(String, String, String, String, String, String, String, String)] = sparkSession.sql(sql).rdd.mapPartitions(itr => {
      implicit val codeMap = codeBroadcast.value;
      val groupMap = groupIdBroadCast.value;
      itr.map(row => {
        val src_cust_id: String = row.getAs("src_cust_id");
        val org_id: String = row.getAs("org_id");
        val group_id = groupMap.getOrElse(org_id, "");
        val src_sys: String = row.getAs("src_sys");
        val src_cert_no: String = row.getAs("src_cert_no");
        val src_cust_type: String = row.getAs("src_cust_type");
        val src_cust_status_bf: String = row.getAs("src_cust_status_bf");
        val src_name: String = row.getAs("src_name");
        var src_certtype: String = null;
        //        证件号需要区分个人法人证件码值,所以需要做特殊处理
        if (row.schema.fieldNames.contains("src_certtype")) {
          src_certtype = row.getAs("src_certtype");
        } else {
          val gr_certtype: String = row.getAs("gr_certtype");
          val fr_certtype: String = row.getAs("fr_certtype");
          src_certtype = if (gr_certtype == null) fr_certtype else gr_certtype;
        }
        (src_cust_id, src_name, src_cert_no, src_certtype, group_id, src_sys, src_cust_type, src_cust_status_bf);
      })
    });
    //过滤客户名称和证件号码都为空的数据
    val filterRdd = srcCustRdd.filter(r => (StringUtils.isNotBlank(r._2) || StringUtils.isNotBlank(r._3)));
    val joinInfo: DataFrame = cmTaskDao.joinInfos(sparkSession, filterRdd, hbaseNamespace);
    val count = joinInfo.count();
    val partitionNum = (count / 10000 + 1).toInt;
    val illegalCerts = illegalCertsBroadcast.value;
    val kvRdd: RDD[(String, Iterable[(String, String, String, String, String, String, String, String, String)])] = cmTaskDao.groupByFlg(joinInfo, partitionNum, illegalCerts);
    cmTaskDao.foreachFlg(kvRdd, hbaseNamespace, illegalCerts);
  }
}
