package server

import java.sql.ResultSet

import dao.impl.DatabaseDaoImpl
import entity.TaskEnvEntity
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description: 任务接口
 */
abstract class TaskServer(taskName: String, taskClass: String, taskOrder: String, sourceSys: String) extends Serializable {
  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass);
  //  private val dateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private val databaseDao = new DatabaseDaoImpl;
  //  任务名
  var tName: String = taskName;
  //  任务实现类
  var tClass: String = taskClass;
  //  任务优先级
  var tOrder: String = taskOrder;
  //  系统
  var sys: String = sourceSys;

  //任务运行方法,自己实现
  def process(taskEntity: TaskEnvEntity);

  //  运行前获取 job_code
  def preProcess(etl_date: String) = {
    //    val beginTime: String = beginTime.format(dateFormat);
    val selectSql = "SELECT job_code FROM a_etl_jobinfo1 WHERE rqbdate ='%s' and job_name='%s'".format(etl_date, taskName);
    val map: mutable.HashMap[String, Integer] = mutable.HashMap[String, Integer]();
    val resultSet: ResultSet = databaseDao.executeQuery(selectSql);
    while (resultSet.next) {
      val job_code: Int = resultSet.getInt("job_code");
      map.put("job_code", job_code);
    }

    if (map.isEmpty) {
      //      执行写入操作
      val addSql = "INSERT INTO a_etl_jobinfo1(rqbdate,job_name,begin_time,status,job_type,job_code) VALUES('%s','%s',%s,'%s','%s','%s')".format(etl_date, taskName, "now()", 2, taskOrder, 1);
      databaseDao.executeUpdate(addSql);
    } else {
      //      执行更新操作
      val updateSql = "UPDATE a_etl_jobinfo1 SET begin_time=%s,end_time=%s,status='%s',job_type='%s',job_code='%s',remark='%s' WHERE rqbdate='%s' and job_name='%s'".format("now()", "null", 2, taskOrder, map.get("job_code").get + 1, "", etl_date, taskName);
      databaseDao.executeUpdate(updateSql);
    }
  }

  //  运行后更新作业运行状态
  def afterProcess(etl_date: String, status: String, ex: String) = {
    //    执行更新操作
    val updateSql = "UPDATE a_etl_jobinfo1 SET end_time=%s,status='%s',job_type='%s',remark='%s' WHERE rqbdate='%s' and job_name='%s'".format("now()", status, taskOrder, ex, etl_date, taskName);
    logger.info("Task run complete,writing log SQL: {}", updateSql);
    databaseDao.executeUpdate(updateSql);
  }
}
