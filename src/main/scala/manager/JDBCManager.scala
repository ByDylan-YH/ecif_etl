package manager

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * Author:BYDylan
 * Date:2020/8/29
 * Description:
 */
class JDBCManager private {
  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass);
  private val JDBCDRIVER: String = "com.mysql.cj.jdbc.Driver";
  private val DATABASEURL: String = "jdbc:mysql://127.0.0.1:3306/ecifdb?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Asia/Shanghai";
//  private val DATABASEURL: String = String.valueOf(BartClient.instance.fetch("ecif.mysql.url"));
  private val USERNAME: String = "root";
//  private val USERNAME: String = String.valueOf(BartClient.instance.fetch("ecif.mysql.username"));
  private val PASSWORD: String = "By9216446o6";
//  private val PASSWORD: String = String.valueOf(BartClient.instance.fetch("ecif.mysql.password"));
  var connection: Connection = _;

  def this(connInfo: mutable.HashMap[String, String]) {
    this;
    if (connInfo != null || connInfo.size > 0) {
      initConnection(connInfo.getOrElse("jdbcDriver", JDBCDRIVER)
        , connInfo.getOrElse("databaseUrl", DATABASEURL)
        , connInfo.getOrElse("userName", USERNAME)
        , connInfo.getOrElse("passWord", PASSWORD));
    } else {
      initConnection();
    }
  }

  def initConnection(jdbcDriver: String = JDBCDRIVER
                     , databaseUrl: String = DATABASEURL
                     , userName: String = USERNAME
                     , passWord: String = PASSWORD
                    ): Unit = {
    try {
      Class.forName(jdbcDriver);
      connection = DriverManager.getConnection(databaseUrl, userName, passWord);
      logger.info("Success login jdbc in: {}", jdbcDriver);
    } catch {
      case e: Exception =>
        logger.error("Failed to initialize connection: {}", e.getMessage);
    }
  }

  def getConnection: Connection = {
    if (connection.isClosed || connection == null) {
      return DriverManager.getConnection(DATABASEURL, USERNAME, PASSWORD);
    } else {
      return connection;
    }
  }

  def getJDBCProperties: Properties = {
    val properties: Properties = new Properties;
    properties.put("user", USERNAME);
    properties.put("password", PASSWORD);
    properties.put("driver", JDBCDRIVER);
    return properties;
  }

}

object JDBCManager {
  private var instance: JDBCManager = null;
  val connInfo: mutable.HashMap[String, String] = mutable.HashMap[String, String]();
  connInfo.put("jdbcDriver", "com.mysql.cj.jdbc.Driver");
  connInfo.put("databaseUrl", "jdbc:mysql://127.0.0.1:3306/ecifdb?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Asia/Shanghai");
  connInfo.put("userName", "root");
  connInfo.put("passWord", "By9216446o6");

  def getInstance(connInfo: mutable.HashMap[String, String] = connInfo): JDBCManager = {
    if (instance == null) {
      instance = new JDBCManager(connInfo);
    }
    return instance;
  }

}

