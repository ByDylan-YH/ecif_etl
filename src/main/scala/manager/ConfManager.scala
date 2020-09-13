package manager

import java.io.File
import java.util

import com.typesafe.config.{Config, ConfigFactory, ConfigList, ConfigObject, ConfigValue}
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.collection.{JavaConverters, mutable}

/**
 * Author:BYDylan
 * Date:2020/8/29
 * Description: 读取 .conf文件
 */
object ConfManager {
  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass);
  var conf: Config = initConf();

  def initConf(): Config = {
    var userPath = System.getProperty("user.dir") + "/tasks/";
    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      userPath = "script/scheduler/daily/tasks/";
    }
    return ConfigFactory.parseFile(new File(userPath + "application.conf")).resolve();
  }

  def getString(name: String): String = {
    try {
      return conf.getString(name);
    } catch {
      case ex: ConfigException => {
        logger.error("Failed to getConfig: {}", ex);
        return null;
      }
    }
  }

  def getAnyRef(name: String): AnyRef = {
    try {
      return conf.getAnyRef(name);
    } catch {
      case ex: ConfigException => {
        logger.error("Failed to getConfig: {}", ex);
        return null;
      }
    }
  }

  def getStringList(name: String): mutable.Buffer[String] = {
    try {
      return JavaConverters.asScalaBuffer(conf.getStringList(name));
    } catch {
      case ex: ConfigException => {
        logger.error("Failed to getConfig: {}", ex);
        return null;
      }
    }
  }

  def getlist(name: String): ConfigList = {
    try {
      return conf.getList(name);
    } catch {
      case ex: ConfigException => {
        logger.error("Failed to getConfig: {}", ex);
        return null;
      }
    }
  }

  def getList(name: String): ListBuffer[String] = {
    try {
      val list: util.Iterator[ConfigValue] = conf.getList(name).iterator()
      var listBuffer: ListBuffer[String] = new ListBuffer[String];
      for (str <- JavaConverters.asScalaIterator(list)) {
        listBuffer += (str + "");
      }
      return listBuffer;
    } catch {
      case ex: ConfigException => {
        logger.error("Failed to getConfig: {}", ex);
        return null;
      }
    }
  }

  def getObject(name: String): ConfigObject = {
    try {
      return conf.getObject(name);
    } catch {
      case ex: ConfigException => {
        logger.error("Failed to getConfig: {}", ex);
        return null;
      }
    }
  }

  def getConfig(name: String): Config = {
    try {
      return conf.getConfig(name);
    } catch {
      case ex: ConfigException => {
        logger.error("Failed to getConfig: {}", ex);
        return null;
      }
    }
  }

  def getConfigList(name: String): util.List[_ <: Config] = {
    try {
      return conf.getConfigList(name);
    } catch {
      case ex: ConfigException => {
        logger.error("Failed to getConfig: {}", ex);
        return null;
      }
    }
  }
}

