package entity

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description: 任务运行环境
 */
case class TaskEnvEntity(sparkSession: SparkSession
                    , illegalCertsBroadcast: Broadcast[ArrayBuffer[String]]
                    , codeBroadcast: Broadcast[mutable.HashMap[String, String]]
                    , priorityBroadcast: Broadcast[mutable.HashMap[String, Integer]]
                    , codePriorityBroadcast: Broadcast[mutable.HashMap[String, Integer]]
                    , groupIdBroadCast: Broadcast[mutable.HashMap[String, String]]
                    , etlDate: String) {
  val hbaseNamespace = s"ecifdb${etlDate}:";
}
