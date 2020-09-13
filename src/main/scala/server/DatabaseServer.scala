package server

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description:
 */
abstract class DatabaseServer {
  def getIllegalCerts: ArrayBuffer[String];
  def getCodeMap: mutable.HashMap[String, String];
  def getGroupIdMap: mutable.HashMap[String, String];
  def getSysPriority: mutable.HashMap[String, Integer];
  def getCodePriority: mutable.HashMap[String, Integer];
}
