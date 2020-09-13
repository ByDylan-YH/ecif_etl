package dao

import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/8/29
 * Description: 数据库查询
 */
abstract class DatabaseDao extends Serializable {
  def executeQuery(sql: String, params: ArrayBuffer[String] = null):ResultSet;

  def executeUpdate(sql: String, params: ArrayBuffer[String] = null): Int;

  def batchExecuteQuery(sql: String, paramsList: ArrayBuffer[ArrayBuffer[String]]): Array[Int];

  def batchExecuteUpdate(sqlList: ArrayBuffer[String]): ArrayBuffer[Int];
}
