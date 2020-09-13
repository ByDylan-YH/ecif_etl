package dao.impl

import java.sql.{Connection, PreparedStatement, ResultSet}

import dao.DatabaseDao
import manager.JDBCManager
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/8/29
 * Description:
 */

class DatabaseDaoImpl extends DatabaseDao with Serializable {
  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass);
  private var affectedRows = 0

  override def executeQuery(sql: String, params: ArrayBuffer[String]): ResultSet = {
    val jdbcManager: JDBCManager = JDBCManager.getInstance();
    val connection: Connection = jdbcManager.getConnection;
    try if (connection != null) {
      val preparedStatement: PreparedStatement = connection.prepareStatement(sql);
      if (params != null && params.length > 0) for (i <- 0 until params.length) {
        preparedStatement.setObject(i + 1, params(i));
      }
      return preparedStatement.executeQuery;
    }
    catch {
      case e: Exception =>
        logger.error("Failed to executeQuery: {}", e.getMessage);
    } finally if (connection != null) jdbcManager.connection = connection;
    return null;
  }

  override def executeUpdate(sql: String, params: ArrayBuffer[String]): Int = {
    val jdbcManager: JDBCManager = JDBCManager.getInstance();
    val connection: Connection = jdbcManager.getConnection;
    try if (connection != null) {
      connection.setAutoCommit(false);
      val preparedStatement: PreparedStatement  = connection.prepareStatement(sql);
      if (params != null && params.length > 0) for (i <- 0 until params.length) {
        preparedStatement.setObject(i + 1, params(i));
      }
      affectedRows = preparedStatement.executeUpdate;
      connection.commit();
    } catch {
      case e: Exception =>
        logger.error("Failed to executeUpdate: {}", e.getMessage);
    } finally if (connection != null) jdbcManager.connection = connection;
    return affectedRows;
  }

  override def batchExecuteUpdate(sqlList: ArrayBuffer[String]): ArrayBuffer[Int] = {
    val jdbcManager: JDBCManager = JDBCManager.getInstance();
    val connection: Connection = jdbcManager.getConnection;
    val returnBuffer: ArrayBuffer[Int] = mutable.ArrayBuffer[Int]();
    try if (connection != null) {
      connection.setAutoCommit(false);
      for (sql <- sqlList) {
        val preparedStatement: PreparedStatement  = connection.prepareStatement(sql);
        affectedRows = preparedStatement.executeUpdate;
        returnBuffer.append(affectedRows);
      }
      connection.commit();
    }
    catch {
      case e: Exception =>
        logger.error("Failed to batchExecuteUpdate: {}", e.getMessage);
        return null
    } finally if (connection != null) jdbcManager.connection = connection;
    return returnBuffer;
  }

  override def batchExecuteQuery(sql: String, paramsList: ArrayBuffer[ArrayBuffer[String]]): Array[Int] = {
    val jdbcManager: JDBCManager = JDBCManager.getInstance();
    val connection: Connection = jdbcManager.getConnection;
    var returnBuffer: Array[Int] = null;
    try if (connection != null) {
      connection.setAutoCommit(false);
      val preparedStatement: PreparedStatement  = connection.prepareStatement(sql);
      if (paramsList != null && paramsList.size > 0) {
        for (params <- paramsList) {
          for (i <- 0 until params.length) {
            preparedStatement.setObject(i + 1, params(i));
          }
          preparedStatement.addBatch();
        }
      }
      returnBuffer = preparedStatement.executeBatch;
      connection.commit();
    }
    catch {
      case e: Exception =>
        logger.error("Failed to executeBatch: {}", e.getMessage);
    } finally if (connection != null) jdbcManager.connection = connection;
    return returnBuffer;
  }
}
