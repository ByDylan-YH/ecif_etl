package server.impl

import java.sql.ResultSet

import dao.DatabaseDao
import dao.impl.DatabaseDaoImpl
import org.apache.commons.lang3.StringUtils
import server.DatabaseServer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description:
 */
class DatabaseServerImpl extends DatabaseServer {
  private val databaseDao: DatabaseDao = new DatabaseDaoImpl;

  //    装入所有的系统优先级到Map集合中初始化
  override def getIllegalCerts: ArrayBuffer[String] = {
    val resultList = new ArrayBuffer[String];
    val sql = "select illegal_cert,status from a_etl_illegal_certs where status='Y';";
    val resultSet: ResultSet = databaseDao.executeQuery(sql);
    while (resultSet.next()) {
      val illegal_cert: String = resultSet.getString("illegal_cert");
      val status: String = resultSet.getString("status");
      if (StringUtils.isNotEmpty(illegal_cert) && StringUtils.isNotEmpty(status)) resultList += (illegal_cert);
    }
    return resultList;
  }

  override def getCodeMap: mutable.HashMap[String, String] = {
    val resultMap: mutable.HashMap[String, String] = mutable.HashMap();
    val sql: String = "SELECT type_id,source_sys,source_code_id,code_id FROM t_sys_code_sourcerelation WHERE status='Y' \n" + "UNION ALL \n" + "SELECT type_id,'S008',code_id AS source_code_id,code_name AS code_id FROM t_sys_code_detail WHERE  type_id = 'S008060';";
    val resultSet: ResultSet = databaseDao.executeQuery(sql);
    while (resultSet.next) {
      val type_id: String = resultSet.getString("type_id");
      val source_sys: String = resultSet.getString("source_sys");
      val source_code_id: String = resultSet.getString("source_code_id");
      val code_id: String = resultSet.getString("code_id");
      if (StringUtils.isNotEmpty(type_id) && StringUtils.isNotEmpty(source_sys) && StringUtils.isNotEmpty(source_code_id) && StringUtils.isNotEmpty(code_id))
        resultMap.put(type_id + "_" + source_sys + "_" + source_code_id, code_id);
    }
    return resultMap;
  }

  override def getGroupIdMap: mutable.HashMap[String, String] = {
    val map: mutable.HashMap[String, String] = mutable.HashMap[String, String]();
    val sql = "select COALESCE(branch_no,org_id) as org_id,group_id from t_sys_org";
    val resultSet: ResultSet = databaseDao.executeQuery(sql);
    while (resultSet.next()) {
      val org_id: String = resultSet.getString("org_id");
      val group_id: String = resultSet.getString("group_id");
      map.put(org_id, group_id);
    }
    map.put("ecif099", "ecif099");
    return map;
  }

  override def getSysPriority: mutable.HashMap[String, Integer] = {
    val resultMap: mutable.HashMap[String, Integer] = mutable.HashMap();
    val sql = "select sys_code,sys_priority,type from t_sys_priority where status='Y';";
    val resultSet: ResultSet = databaseDao.executeQuery(sql);
    while (resultSet.next()) {
      val sys_code: String = resultSet.getString("sys_code");
      val sys_type: String = resultSet.getString("type");
      val sys_priority: Integer = resultSet.getInt("sys_priority");
      resultMap.put(sys_code + "_" + sys_type, sys_priority);
    }
    return resultMap;
  }

  override def getCodePriority: mutable.HashMap[String, Integer] = {
    val resultMap: mutable.HashMap[String, Integer] = mutable.HashMap();
    val sql = "select type_id,code_id,field_priority from t_col_field_priority where status='Y'";
    val resultSet: ResultSet = databaseDao.executeQuery(sql);
    while (resultSet.next()) {
      val type_id: String = resultSet.getString("type_id");
      val code_id: String = resultSet.getString("code_id");
      val sys_priority: Integer = resultSet.getInt("field_priority");
      resultMap.put(type_id + "_" + code_id, sys_priority);
    }
    return resultMap;
  }

}
