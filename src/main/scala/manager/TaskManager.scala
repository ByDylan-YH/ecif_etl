package manager

import java.sql.ResultSet

import com.xyzq.simpson.bart.client.Environment
import dao.DatabaseDao
import dao.impl.DatabaseDaoImpl
import entity.TaskPoolEntity
import org.json4s.{JValue, NoTypeHints}
import org.json4s.jackson.JsonMethods.{mapper, parse}
import org.json4s.jackson.Serialization
import org.slf4j.{Logger, LoggerFactory}
import server.TaskServer

import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/8/29
 * Description:
 */
object TaskManager {
  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass);
  private val databaseDao: DatabaseDao = new DatabaseDaoImpl;
  var taskPool = initTaskPool();

  //  def main(args: Array[String]): Unit = {
  //    initTaskPool
  //  }

  def initTaskPool(): TaskPoolEntity = {
    val taskNameList = ConfManager.getStringList("app.task");
    //    logger.info("initTaskPool,taskNameList: {}", taskNameList);
    val taskPool: TaskPoolEntity = new TaskPoolEntity;
    for (task <- taskNameList) {
      var taskClass: String = ConfManager.getString(task.concat(".taskClass"));
      if (Environment.getEnvironment == "test") taskClass = taskClass.replace("IMBulkLoadTask", "IMTask");
      val clazz: Class[_] = Class.forName(taskClass);
      val taskAllParmeters = ConfManager.getAnyRef(task);
      val taskDao: TaskServer = parseTaskToObject(taskAllParmeters, clazz).asInstanceOf[TaskServer];
      taskPool.addTask2Pool(task, taskDao);
    }
    return taskPool;
  }

  def getTaskByType(taskType: String, etlDate: String): Array[TaskServer] = {
    val doneList: ArrayBuffer[String] = getDoneTask(etlDate);
    logger.info("GetDoneTaskList: {}", doneList);
    logger.info("Application taskLi: {}", this.taskPool.taskMap.keySet);
    return this.taskPool.taskMap.values.filter(task => {
      (!doneList.contains(task.tName)) && taskType == task.tOrder;
    }).toArray.sortWith((a, b) => {
      a.tName < b.tName;
    })
  }

  def getTaskByName(taskName: String): TaskServer = {
    return this.taskPool.taskMap(taskName);
  }

  def getDoneTask(etlDate: String): ArrayBuffer[String] = {
    val selectSql = "select job_name from a_etl_jobinfo1 where status = 1 and date_format(rqbdate, '%Y%m%d') ='" + etlDate + "';";
    logger.info("getDoneTask,selectSql: {}", selectSql);
    val list = new ArrayBuffer[String]();
    val resultSet: ResultSet = databaseDao.executeQuery(selectSql);
    while (resultSet.next) {
      val job_name = resultSet.getString("job_name");
      list.+=(job_name);
    }
    return list;
  }

  def clearHistroy(etlDate: String): Int = {
    val delSql = "DELETE FROM a_etl_jobinfo1 WHERE date_format(rqbdate, '%Y%m%d') ='" + etlDate + "';";
    return databaseDao.executeUpdate(delSql);
  }

  def getTaskByTypeAndSrcSys(taskOrder: String, sourceSys: String): Array[TaskServer] = {
    return this.taskPool.taskMap.values.map(task => {
      if (task.sys == "ecif") {
        task.sys = sourceSys;
      }
      task;
    }).filter(task => {
      task.tOrder == taskOrder && (task.sys == sourceSys || task.sys == "ecif");
    }).toArray.sortWith((a, b) => {
      a.tName < b.tName;
    })
  }

  def getAllTask(etlDate: String): Array[TaskServer] = {
    val list = getDoneTask(etlDate);
    val values = this.taskPool.taskMap.values.filter(task => {
      (!list.contains(task.tName));
    }).toArray.sortWith((a, b) => {
      if (a.tOrder == b.tOrder) {
        a.tName < b.tName;
      } else {
        a.tOrder < b.tOrder;
      }
    })
    return values;
  }

  private def parseTaskToObject[T: Manifest](confRef: AnyRef, clazz: Class[T]): T = {
    implicit val formats = Serialization.formats(NoTypeHints);
    implicit val mf: Manifest[T] = Manifest.classType(clazz);
    val json: String = mapper.writer().writeValueAsString(confRef);
    val value: JValue = parse(json);
    value.extract[T](formats, mf);
  }
}
