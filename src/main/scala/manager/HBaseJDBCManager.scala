package manager

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.security.UserGroupInformation
import java.io.IOException
import java.security.PrivilegedExceptionAction

import org.slf4j.{Logger, LoggerFactory}

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description: HBase 连接类
 */
object HBaseJDBCManager extends Serializable {
  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass);
   private val conf = HBaseConfiguration.create;
  private var KERBEROS_SWITCH = true;
  private var connection: Connection = null;

  def getConnection: Connection = {
    if (connection == null || connection.isClosed) {
      conf.set(HConstants.ZOOKEEPER_QUORUM, ProManager.zkQuorum);
      conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, ProManager.zkPort);
      conf.set(HConstants.CLUSTER_DISTRIBUTED, "true");
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, ProManager.zNode);
      conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
      conf.setBoolean(HConstants.HBASE_RS_NONCES_ENABLED, false);
      conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 31);
      conf.setLong(HConstants.HBASE_CLIENT_PAUSE, 1000L);
      try if (KERBEROS_SWITCH) {
        //        System.setProperty("java.security.krb5.conf", ProManager.krb5);
        //        conf.set("hadoop.security.authentication", "kerberos");
        //        conf.set("hbase.security.authentication", "kerberos");
        //        conf.set("hbase.master.kerberos.principal", ProManager.hbPrincipal);
        //        conf.set("hbase.regionserver.kerberos.principal", ProManager.kbPrincipal);
        UserGroupInformation.setConfiguration(conf);
        //        UserGroupInformation.loginUserFromKeytab(ProManager.kbPrincipal, ProManager.keytab);
        val loginUser: UserGroupInformation = UserGroupInformation.getLoginUser;
        connection = loginUser.doAs(
          new PrivilegedExceptionAction[Connection]() {
            override def run: Connection = {
              return ConnectionFactory.createConnection(conf);
            }
          }
        ).asInstanceOf[Connection];
      }
      else connection = ConnectionFactory.createConnection(conf)
      catch {
        case e: IOException =>
          logger.error("Failed to get hbase connect: {}", e.getMessage);
      }
    }
    return connection;
  }
}
