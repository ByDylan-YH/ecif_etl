package manager

import java.util.{Locale, ResourceBundle}

/**
 * Author:BYDylan
 * Date:2020/8/29
 * Description: 读取 .properties 文件
 */
object ProManager {
  private val resourceBundle = ResourceBundle.getBundle("application", new Locale("zh_CN", "CN"));
  val zkQuorum: String = ProManager.getString("hbase.zookeeper.quorum");
  val zkPort: String = ProManager.getString("hbase.zookeeper.property.clientPort");
  val zNode: String = ProManager.getString("zookeeper.znode.parent");
  val hbPrincipal: String = ProManager.getString("hbase.master.kerberos.principal");
  val kbPrincipal: String = ProManager.getString("kerberos.principal");
  val keytab: String = ProManager.getString("kerberos.keytab");
  val krb5: String = ProManager.getString("java.security.krb5.conf");
  val hbaseNameSpace: String = ProManager.getString("hbase.namespace");
  val hbaseColumnFamily: String = ProManager.getString("hbase.column.family");

  def getString(key: String): String = resourceBundle.getString(key);
}

