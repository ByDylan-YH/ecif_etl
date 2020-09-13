package main

import manager.{TaskManager, UDFManager}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import server.{DatabaseServer, TaskServer}
import server.impl.DatabaseServerImpl
import entity.TaskEnvEntity
import org.apache.log4j.Level

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/8/28
 * Description:
 * S008: 20191201 cm_o_s008_bib_t_cm_clientinfo 0
 * S015: 20191201 cm_o_s015_tg_t_dc_glr_info 0
 * Fix : 20191201 fix_multi_custid_task 0
 */
object StartMain {
  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass);
  //  org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
  private val systemInformation: String = System.getProperties.getProperty("os.name");
  private val project_path: String = System.getProperty("user.dir");
  System.setProperty("HADOOP_USER_NAME", "root");
  //  System.setProperty("java.security.krb5.conf", ProManager.krb5);
  System.setProperty("simpson.environment", "test,192.168.1.201:2181");

  //  System.setProperty("simpson.environment", "test,192.168.1.201:2181")
  def main(args: Array[String]) {
    val databaseServer: DatabaseServer = new DatabaseServerImpl;
    if (args.length < 3) {
      logger.error("Require three parameters : etlDate taskName taskOrder");
      throw new Exception("Three parameters are required , but found: " + args.length);
    }
    val etlDate: String = args(0);
    val taskName: String = args(1);
    val taskOrder: String = args(2);
    logger.info("systemInformation: {}", systemInformation);
    logger.info("etlDate: {}, taskName: {}, taskOrder: {}", etlDate, taskName, taskOrder);
    val sparkConf: SparkConf = new SparkConf().setAppName("ecif_etl_yh").set("spark.shuffle.consolidateFiles", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.testing.memory", "4294960000")
      .set("spark.cores.max", "4")
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.network.timeout", "30")
    //      .set("spark.ui.showconsoleeprogress", "true")
    //      .set("hive.metastore.urls", "thrift://192.168.1.201:9083")
    //      .setMaster("spark://by202:7077");
    if (systemInformation.contains("Window")) {
      sparkConf.setMaster("local[*]");
    }
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
    val sc: SparkContext = sparkSession.sparkContext;
    //    sc.setLogLevel("WARN"); // ALL,DEBUG,ERROR,FATAL,TRACE,WARN,INFO,OFF
    org.apache.log4j.Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN);
    UDFManager.register(sparkSession);
    //    spark 3.0 能够识别句末分号,2.+不行
    sparkSession.sql("select client_id,create_user_org,oc_date from ods_bib.t_ods_bib_t_pm_project_ibma_hv where part_init_date like '201912%';").show(1);
    logger.info("Query hive success");
    val illegalCerts: ArrayBuffer[String] = databaseServer.getIllegalCerts;
    val codeMap: mutable.HashMap[String, String] = databaseServer.getCodeMap;
    val groupIdMap: mutable.HashMap[String, String] = databaseServer.getGroupIdMap;
    val priorityMap: mutable.HashMap[String, Integer] = databaseServer.getSysPriority;
    val codePriorityMap: mutable.HashMap[String, Integer] = databaseServer.getCodePriority;
    val illegalCertsBroadcast: Broadcast[ArrayBuffer[String]] = sc.broadcast(illegalCerts);
    val groupIdBroadCast: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(groupIdMap);
    val codeBroadcast: Broadcast[mutable.HashMap[String, String]] = sc.broadcast(codeMap);
    val priorityBroadcast: Broadcast[mutable.HashMap[String, Integer]] = sc.broadcast(priorityMap);
    val codePriorityBroadcast: Broadcast[mutable.HashMap[String, Integer]] = sc.broadcast(codePriorityMap);
    //        val conf = new Configuration;
    //        conf.set("hadoop.security.authentication", "kerberos");
    //        UserGroupInformation.setConfiguration(conf);
    //        UserGroupInformation.loginUserFromKeytab(ProManager.kbPrincipal, ProManager.keytab);
    //        val loginUser: UserGroupInformation = UserGroupInformation.getLoginUser;
    //        if (!loginUser.toString.contains(ProManager.kbPrincipal)) {
    //          logger.error("Sign in Hive failed: {}", loginUser);
    //        }
    val taskEnv = new TaskEnvEntity(sparkSession
      , illegalCertsBroadcast
      , codeBroadcast
      , priorityBroadcast
      , codePriorityBroadcast
      , groupIdBroadCast
      , etlDate);
    val waitRunTaskArray: Array[TaskServer] = TaskManager.getTaskByType("1", etlDate);
    logger.info("Runing taskOrder: {}", taskOrder);
    logger.info("Waiting runing task size: {}", waitRunTaskArray.size);

    if (!taskName.equals("0")) {
      val task = TaskManager.getTaskByName(taskName);
      execTask(task, taskEnv);
    } else if (!taskOrder.equals("0")) {
      val tasks = TaskManager.getTaskByType(taskOrder, etlDate);
      for (task <- tasks) {
        execTask(task, taskEnv);
      }
    } else {
      val tasks = TaskManager.getAllTask(etlDate).filter(t => !"nm_a_etl_namespace | rollback | mysql_sync".contains(t.tName));
      for (task <- tasks) {
        execTask(task, taskEnv);
      }
    }
    sparkSession.close();
  }

  def execTask(taskDetailDao: TaskServer, taskEntity: TaskEnvEntity): Unit = {
    logger.info("Runing taskName: {}", taskDetailDao.tName);
    taskDetailDao.preProcess(taskEntity.etlDate);
    try {
      taskDetailDao.process(taskEntity);
    } catch {
      case ex: Exception => {
        // 任务进行中是2, 失败是3, 成功是1
        taskDetailDao.afterProcess(taskEntity.etlDate, "3", ex.getMessage.replace("'", ""));
        logger.error("Run task failed: {}", ex.getMessage);
        ex.printStackTrace();
        return;
      }
    }
    taskDetailDao.afterProcess(taskEntity.etlDate, "1", "");
  }
}
