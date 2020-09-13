package dao

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/9/12
 * Description:
 */
abstract class TaskDao {
  //  判断证件号是否为空
  def isCertNotNull(str: String): Boolean = {
    val strs = str.split("#ecif-sep#", -1);
    val cert_no = strs(1).trim;
    return cert_no.nonEmpty;
  }

  //  判断三要素是否合法
  def isCertInfoLegal(name: String, cert_no: String, cert_type_cd: String, illegalCerts: ArrayBuffer[String]): Boolean = {
    StringUtils.isNotBlank(name) && StringUtils.isNotBlank(cert_no) && StringUtils.isNotBlank(cert_type_cd);
  }

  def isCertInfoLegal(str: String): Boolean = {
    val strs = str.split("#ecif-sep#", -1);
    val name = strs(0).trim;
    val cert_no = strs(1).trim;
    val cert_type_cd_with_ct = strs(2).trim;
    var cert_type_cd = "";
    if (cert_type_cd_with_ct.contains("#ecif-ct-sep#")) { // 如果存在这个分隔符, 则说明证件类型需要转码, type_id为后面的
      cert_type_cd = cert_type_cd_with_ct.split("#ecif-ct-sep#", -1)(0);
    } else {
      cert_type_cd = cert_type_cd_with_ct;
    }
    StringUtils.isNotBlank(name) && StringUtils.isNotBlank(cert_no) && StringUtils.isNotBlank(cert_type_cd);
  }

  def filterNull(nits: Array[String]): Array[String] = {
    // 如果都不合法就是临时客户, 临时客户存所有证件号不为空的记录, 如果证件号都为空随便存一条
    // 正式客户
    val isZS = nits.exists(isCertInfoLegal);
    if (isZS) {
      // 如果是正式客户, 返回所有有证件号的
      nits.filter(isCertInfoLegal);
    } else {
      //临时客户存所有证件号不为空的记录, 如果证件号都为空随便存一条
      if (nits.exists(isCertNotNull)) {
        // 存在证件号不为空的临时客户
        nits.filter(isCertNotNull);
      } else {
        //证件号都为空随便存一条
        Array(nits(0));
      }
    }
  }

  def transform(str: String)(implicit sourceSys: String, codeMap: mutable.HashMap[String, String]): String;

  def foreachFlg(custFlgRdd: RDD[(String, Iterable[(String, String, String, String, String, String, String, String, String)])],
                 hbaseNamespace: String, illegalCerts: ArrayBuffer[String]): Unit;

  def joinInfos(sparkSession: SparkSession,
                srcCustRdd: RDD[(String, String, String, String, String, String, String, String)],
                hbaseNamespace: String): DataFrame;

  def groupByFlg(joinResult: DataFrame, partitionNum: Int, illegalCerts: ArrayBuffer[String]):
  RDD[(String, Iterable[(String, String, String, String, String, String, String, String, String)])];
}
