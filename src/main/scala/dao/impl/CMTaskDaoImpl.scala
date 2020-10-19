package dao.impl

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util
import java.util.UUID

import dao.{HBaseDao, TaskDao}
import entity.CustTypeEntity
import entity.hbase.{PTY_CUST_FLGEntity, PTY_CUST_GROUPEntity, PTY_SRC_CUST_IDEntity}
import manager.{HBaseJDBCManager, ParaManager, ProManager}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import utils.EncryptUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description:
 */
class CMTaskDaoImpl extends TaskDao with Serializable {
  private var etlDate: String = null;
  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass);
  var hbaseDao: HBaseDao = _;

  def this(etlDate: String) {
    this
    this.etlDate = etlDate;
    hbaseDao = new HBaseDaoImpl(etlDate);
  }

  //  对三要素拼接的字符串，取证件类型转码
  override def transform(str: String)(implicit sourceSys: String, codeMap: mutable.HashMap[String, String]): String = {
    val strs = str.split("#ecif-sep#", -1);
    val cert_type_cd_with_ct = strs(2).trim;
    var cert_type_cd = "";
    if (cert_type_cd_with_ct.contains("#ecif-ct-sep#")) { // 如果存在这个分隔符, 则说明证件类型需要转码, type_id为后面的
      val cs = cert_type_cd_with_ct.split("#ecif-ct-sep#", -1);
      cert_type_cd = cs(0);
      val type_id = cs(1);
      if (cert_type_cd == null || cert_type_cd.trim.isEmpty) {
        strs(2) = cert_type_cd;
      } else {
        //码值转换失败之后,用$$${}$$$做标记，并以typeId_sourceSys_value格式保存原值,方便之后补数
        strs(2) = codeMap.getOrElse(type_id + "_" + sourceSys + "_" + cert_type_cd,
          "$$${" + type_id + "_" + sourceSys + "_" + cert_type_cd + "}$$$");
      }
      return strs.mkString("#ecif-sep#");
    } else {
      return str;
    }
  }

  override def foreachFlg(custFlgRdd: RDD[(String, Iterable[(String, String, String, String, String, String, String, String, String)])],
                          hbaseNamespace: String, illegalCerts: ArrayBuffer[String]): Unit = {
    custFlgRdd.foreachPartition(itr => {
      val ptyCustFlgCache: ArrayBuffer[PTY_CUST_FLGEntity] = ArrayBuffer[PTY_CUST_FLGEntity]();
      //      val ptyCustFlgCacheTableName: String = annotationUtils.getClassAnnotation[PTY_CUST_FLGEntity, HBaseTable];
      val ptySrcCustIdCache: ArrayBuffer[PTY_SRC_CUST_IDEntity] = ArrayBuffer[PTY_SRC_CUST_IDEntity]();
      val ptyCustGroupCache: ArrayBuffer[PTY_CUST_GROUPEntity] = ArrayBuffer[PTY_CUST_GROUPEntity]();
      val delPtySrcCustIdCache: ArrayBuffer[String] = ArrayBuffer[String]();
      val delPtyCustFlgCache: ArrayBuffer[String] = ArrayBuffer[String]();
      itr.foreach(tuple => {
        val flgs = tuple._1.split("#ecif-sep#");
        val src_name = flgs(0);
        val src_cert_no = flgs(1);
        val src_certtype = flgs(2);
        val isFlgLegal = isCertInfoLegal(src_name, src_cert_no, src_certtype, illegalCerts);
        var cust_id: String = null;
        //对相同三要素的客户进行遍历
        tuple._2.foreach(custInfo => {
          val cust_type_cd = custInfo._1;
          val rel_cust_id = custInfo._2;
          val src_cust_id = custInfo._3;
          val group_id = custInfo._4;
          val src_sys = custInfo._5;
          val src_cust_type = custInfo._6;
          val stock_cust_id = custInfo._7;
          val flg_cust_id = custInfo._8;
          val src_cust_status_bf = custInfo._9;

          if (rel_cust_id == null) {
            if (flg_cust_id == null) {
              //情景1 完全新客户
              if (isFlgLegal) {
                if (cust_id == null) {
                  cust_id = getNewCustId(if (src_cust_type == "01") "GR" else "FR");
                }
                ptySrcCustIdCache.+=(toSrcCustId(cust_id, src_cust_id, src_sys, src_cust_type));
                ptyCustFlgCache.+=(toCustFlg(cust_id, src_cert_no, src_certtype, src_name, src_cust_type));
                ptyCustGroupCache.+=(toCustGroup(cust_id, src_sys, src_cust_id, group_id, src_cust_status_bf));
              } else {
                if (cust_id == null) {
                  cust_id = getNewCustId("LS");
                }
                val cust_type_cd = transCustType(src_cust_type, isFlgLegal);
                ptySrcCustIdCache.+=(toSrcCustId(cust_id, src_cust_id, src_sys, cust_type_cd));
                ptyCustGroupCache.+=(toCustGroup(cust_id, src_sys, src_cust_id, group_id, src_cust_status_bf));
              }
            } else {
              //情景2 三要素已经存在已分配客户号 这里的都是正式客户
              ptySrcCustIdCache.+=(toSrcCustId(flg_cust_id, src_cust_id, src_sys, src_cust_type));
              ptyCustGroupCache.+=(toCustGroup(flg_cust_id, src_sys, src_cust_id, group_id, src_cust_status_bf));
            }
          } else { // rel_cust_id != null
            if (stock_cust_id != null) {
              //情景3，因为三要素表不存非法三要素，所以能进入这个if的客户一定是正式客户
              // 判断当前三要素是否合法,如果三要素不合法，先删除当前这条三要素，然后再查询该客户的所有三要素，再对每一个三要素进行合法性判断，假如每一个三要素都不合法或者已经没有其他三要素，才会把该客户修改为临时客户
              //              ptyCustGroupCache.add(toCustGroup(rel_cust_id, src_sys, src_cust_id, group_id))
              //情景3
              //是老客户，且三要素与之前一致，此时判断三要素是否合法,合法的话原逻辑走，不合法则删除原客户号记录并生成临时客户
              if (isFlgLegal) {
                ptyCustGroupCache.+=(toCustGroup(rel_cust_id, src_sys, src_cust_id, group_id, src_cust_status_bf));
              } else {
                val result: util.Iterator[Result] = hbaseDao.scaneByPrefixFilter("ecifdb" + etlDate + ":PTY_CUST_FLG", stock_cust_id.reverse);
                var flag = true // true 为要改成临时客户
                while (result.hasNext) {
                  val row = result.next();
                  val cust_type = row.getValue(ProManager.hbaseColumnFamily.getBytes(), "cust_type_cd".getBytes());
                  val cert_no = row.getValue(ProManager.hbaseColumnFamily.getBytes(), "cert_no".getBytes());
                  val name = row.getValue(ProManager.hbaseColumnFamily.getBytes(), "name".getBytes());
                  if ((!new String(cert_no).equals(src_cert_no)) && isCertInfoLegal(new String(name), new String(cert_no), new String(cust_type), illegalCerts)) {
                    flag = false;
                  }
                }
                if (flag && CustTypeEntity.checkIsValidCust(cust_type_cd)) {
                  val srcCustIdKey = rel_cust_id.reverse + EncryptUtils.md5Encrypt32(src_cust_id + src_sys);
                  delPtySrcCustIdCache.+=(srcCustIdKey);
                  val srcCustFlgKey = rel_cust_id.reverse + EncryptUtils.md5Encrypt32(src_name + src_cert_no + src_certtype);
                  delPtyCustFlgCache.+=(srcCustFlgKey);
                  if (cust_id == null) {
                    cust_id = getNewCustId("LS");
                  }
                  val cust_type_cd = transCustType(src_cust_type, isFlgLegal);
                  val srcCustIdTuple = toSrcCustId(cust_id, src_cust_id, src_sys, cust_type_cd);
                  ptySrcCustIdCache.+=(srcCustIdTuple);
                  ptyCustGroupCache.+=(toCustGroup(cust_id, src_sys, src_cust_id, group_id, src_cust_status_bf));
                } else {
                  val srcCustFlgKey = rel_cust_id.reverse + EncryptUtils.md5Encrypt32(src_name + src_cert_no + src_certtype);
                  delPtyCustFlgCache.+=(srcCustFlgKey);
                }
              }
            } else { // rel_cust_id != null && stock_cust_id == null
              if (flg_cust_id == null) {
                //情景4 源系统客户号+源系统一样, 但是三要素不一样, 三要素修改了 移除pty_src_cust_id中的关系, 生成新的客户号
                if (isFlgLegal) {
                  if (cust_id == null) {
                    cust_id = getNewCustId(if (src_cust_type == "01") "GR" else "FR");
                  }
                  val srcCustIdKey = rel_cust_id.reverse + EncryptUtils.md5Encrypt32(src_cust_id + src_sys);
                  if (src_cust_type != "01") {
                    val custIdList: util.List[String] = hbaseDao.getColumnValueFilter("ecifdb20191201:PTY_SRC_CUST_ID"
                      , ProManager.hbaseColumnFamily
                      , "cust_id"
                      , "src_cust_id"
                      , src_cust_id);
                    //                    这张表默认只有1个custId,但列值过滤器返回的是一个list,先默认取第1个元素
                    val custId: String = custIdList.get(0);
                    val certTypeCdList: util.List[String] = hbaseDao.getColumnValueFilter("ecifdb20191201:PTY_CUST_FLG"
                      , ProManager.hbaseColumnFamily
                      , "cert_type_cd"
                      , "cust_id"
                      , custId);
                    //                    如果本次生成的 cert_type_cd 能在之前的表找到,那么就执行删除操作
                    println(src_cust_id + " : " + certTypeCdList + " : " + src_certtype);
                    if (certTypeCdList.contains(src_certtype)) {
                      delPtySrcCustIdCache.+=(srcCustIdKey);
                    }
                  } else {
                    delPtySrcCustIdCache.+=(srcCustIdKey);
                  }
                  ptyCustFlgCache.+=(toCustFlg(cust_id, src_cert_no, src_certtype, src_name, src_cust_type));
                  ptyCustGroupCache.+=(toCustGroup(cust_id, src_sys, src_cust_id, group_id, src_cust_status_bf));
                  if (cust_type_cd == "01" || cust_type_cd == "02") { // 原来是正式客户, 现在依旧是, 更新记录
                    ptySrcCustIdCache.+=(toSrcCustId(cust_id, src_cust_id, src_sys, src_cust_type));
                  } else { // 原来是临时, 现在修复为正式
                    ptySrcCustIdCache.+=(toSrcCustId(cust_id, src_cust_id, src_sys, src_cust_type));
                  }
                } else { //正式客户重新生成客户号
                  if (!rel_cust_id.startsWith("LS")) {
                    val srcCustIdKey = rel_cust_id.reverse + EncryptUtils.md5Encrypt32(src_cust_id + src_sys);
                    delPtySrcCustIdCache.+=(srcCustIdKey);
                    if (cust_id == null) {
                      cust_id = getNewCustId("LS");
                    }
                    val cust_type_cd = transCustType(src_cust_type, isFlgLegal);
                    ptySrcCustIdCache.+=(toSrcCustId(cust_id, src_cust_id, src_sys, cust_type_cd));
                    ptyCustGroupCache.+=(toCustGroup(cust_id, src_sys, src_cust_id, group_id, src_cust_status_bf));
                  } else {
                    ptyCustGroupCache.+=(toCustGroup(rel_cust_id, src_sys, src_cust_id, group_id, src_cust_status_bf));
                  }
                }
              } else { // rel_cust_id != null && stock_cust_id == null && flg_cust_id != null
                //情景5 修改了三要素, 而且新的三要素原来系统里面依旧分配过客户号
                val srcCustIdKey = rel_cust_id.reverse + EncryptUtils.md5Encrypt32(src_cust_id + src_sys);
                delPtySrcCustIdCache.+=(srcCustIdKey);
                ptySrcCustIdCache.+=(toSrcCustId(flg_cust_id, src_cust_id, src_sys, src_cust_type));
                ptyCustGroupCache.+=(toCustGroup(flg_cust_id, src_sys, src_cust_id, group_id, src_cust_status_bf));
              }
            }
          }
        })
      })
      hbaseDao.delete(classOf[PTY_SRC_CUST_IDEntity], delPtySrcCustIdCache); // 终极bug
      hbaseDao.delete(classOf[PTY_CUST_FLGEntity], delPtyCustFlgCache);
      hbaseDao.save(ptyCustFlgCache);
      hbaseDao.save(ptySrcCustIdCache);
      hbaseDao.save(ptyCustGroupCache);

      ptyCustFlgCache.clear();
      ptyCustFlgCache.clear();
      ptySrcCustIdCache.clear();
      ptyCustGroupCache.clear();
      delPtySrcCustIdCache.clear();
    })
  }

  def getPtySrcCustIdDtf(table: String,
                         sparkContext: SparkContext,
                         etl_date: String): RDD[(String, String, String, String)] = {
    val hBaseConf: Configuration = HBaseJDBCManager.getConnection.getConfiguration;
    hBaseConf.set(TableInputFormat.INPUT_TABLE, table); // 源系统和ecif对照表

    // 从数据源获取数据
    val pty_src_cust_id = sparkContext.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    pty_src_cust_id.map(r => (
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cust_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cust_type_cd"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("src_cust_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("src_sys")))
    ));
  }

  def getPtyCustFlgDtf(table: String,
                       sparkContext: SparkContext, etl_date: String): RDD[(String, String, String, String)] = {
    val hBaseConf: Configuration = HBaseJDBCManager.getConnection.getConfiguration;
    //    源系统和ecif对照表
    hBaseConf.set(TableInputFormat.INPUT_TABLE, table);
    // 从数据源获取数据
    val pty_src_cust_id = sparkContext.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]);
    // 将数据映射为表  也就是将 RDD转化为 cust_id,name,cert_type_cd,cert_no
    pty_src_cust_id.map(r => (
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cust_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("name"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cert_type_cd"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes(ProManager.hbaseColumnFamily), Bytes.toBytes("cert_no")))
    ));
  }

  /**
   * mapPartitions是将关联后的数据转为key-value形式,key为三要素拼接的字符串,value时其他所有信息。当客户三要素信息不同时，
   * 需做特殊处理(拼接UUID)，识别成单独客户。groupByKey将相同三要素聚合在一起做处理
   */
  override def groupByFlg(joinResult: DataFrame, partitionNum: Int, illegalCerts: ArrayBuffer[String]):
  RDD[(String, Iterable[(String, String, String, String, String, String, String, String, String)])] = {
    joinResult.rdd.mapPartitions(itr => {
      itr.map(row => {
        val cust_id = row.getAs[String]("rel_cust_id");
        val src_cust_id = row.getAs[String]("src_cust_id");
        val group_id = row.getAs[String]("group_id");
        val src_cert_no = row.getAs[String]("src_cert_no");
        val src_sys = row.getAs[String]("src_sys");
        val src_cust_type = row.getAs[String]("src_cust_type");
        val src_cust_status_bf = row.getAs[String]("src_cust_status_bf");
        val src_name = row.getAs[String]("src_name");
        val src_certtype = row.getAs[String]("src_certtype");
        val stock_cust_id = row.getAs[String]("stock_cust_id");
        val flg_cust_id = row.getAs[String]("flg_cust_id");
        val cust_type_cd = row.getAs[String]("cust_type_cd");
        val isFlgLegal = isCertInfoLegal(src_name, src_cert_no, src_certtype, illegalCerts);
        val tmpAppender = if (isFlgLegal) {
          ""
        } else {
          UUID.randomUUID();
        }
        (src_name + "#ecif-sep#" + src_cert_no + "#ecif-sep#" + src_certtype + "#ecif-sep#" + tmpAppender,
          (cust_type_cd, cust_id, src_cust_id, group_id, src_sys, src_cust_type, stock_cust_id, flg_cust_id, src_cust_status_bf));
      });
    }).groupByKey(partitionNum);
  }

  /**
   * 用源系统关联PTY_SRC_CUST_ID和PTY_CUST_FLG表
   *
   * 关联1：用源系统客户号src_cust_id和src_sys关联PTY_SRC_CUST_ID表，假如能关联上说明源系统客户之前进入过ecif系统，
   * 取rel_cust_id为能否关联的标志
   *
   * 关联2：用关联1的结果取源系统客户三要素和rel_cust_id关联PTY_CUST_FLG，假如能关联上说明这个源系统客户的三要素没有做改动
   * 取stock_cust_id为能否关联的标志
   *
   * 关联3：用关联2的结果,取源系统客户三要素关联PTY_CUST_FLG，能关联上说明已经为这个三要素分配过ecif客户号
   * 取flg_cust_id为能否关联的标志
   */
  override def joinInfos(sparkSession: SparkSession,
                         srcCustRdd: RDD[(String, String, String, String, String, String, String, String)],
                         hbaseNamespace: String): DataFrame = {
    val sparkContext = sparkSession.sparkContext;
    val custDtf: DataFrame = sparkSession.createDataFrame(srcCustRdd).toDF("src_cust_id", "src_name", "src_cert_no", "src_certtype", "group_id", "src_sys", "src_cust_type", "src_cust_status_bf");
    logger.info("过滤空值后转成DataFrame,custDtf: : {}", custDtf.show(300));
    //    读取源pty_src_cust_id
    val value: RDD[(String, String, String, String)] = getPtySrcCustIdDtf(hbaseNamespace + "PTY_SRC_CUST_ID", sparkContext, etlDate);
    val ptySrcCustIdDtf: DataFrame = sparkSession.createDataFrame(getPtySrcCustIdDtf(hbaseNamespace + "PTY_SRC_CUST_ID", sparkContext, etlDate)).toDF("cust_id", "cust_type_cd", "src_cust_id", "src_sys");
    logger.info("读取源pty_src_cust_id,ptySrcCustIdDtf: {}", ptySrcCustIdDtf.show(300));

    //    读取三要素信息
    val ptyCustFlgDtf = sparkSession.createDataFrame(getPtyCustFlgDtf(hbaseNamespace + "PTY_CUST_FLG", sparkContext, etlDate)).toDF("cust_id", "name", "cert_type_cd", "cert_no");
    logger.info("读取三要素信息,ptyCustFlgDtf: {}", ptyCustFlgDtf.show(300));

    val ptyCustGroupDtf = sparkSession.createDataFrame(getPtyCustFlgDtf(hbaseNamespace + "PTY_CUST_GROUP", sparkContext, etlDate)).toDF("cust_id", "src_cust_id", "group_id", "bk_id");
    logger.info("读取三要素信息,ptyCustGroupDtf: {}", ptyCustGroupDtf.show(300));

    //    关联1
    val joinSrcCustIdDtf = ptySrcCustIdDtf.join(custDtf, Seq("src_cust_id", "src_sys"), "right").withColumnRenamed("cust_id", "rel_cust_id");
    logger.info("关联1: {}", joinSrcCustIdDtf.show(300));

    //    关联2
    val joinFlgByCustIdDtf = joinSrcCustIdDtf.join(ptyCustFlgDtf
      , joinSrcCustIdDtf("rel_cust_id") === ptyCustFlgDtf("cust_id")
        && joinSrcCustIdDtf("src_name") === ptyCustFlgDtf("name")
        && joinSrcCustIdDtf("src_certtype") === ptyCustFlgDtf("cert_type_cd")
        && joinSrcCustIdDtf("src_cert_no") === ptyCustFlgDtf("cert_no"), "left")
      .withColumnRenamed("cust_id", "stock_cust_id")
      .drop("name").drop("cert_no").drop("cert_type_cd");
    logger.info("关联2: {}", joinFlgByCustIdDtf.show(300));

    //关联3
    val joinFlgWithFlg = joinFlgByCustIdDtf.join(ptyCustFlgDtf
      , joinFlgByCustIdDtf("src_name") === ptyCustFlgDtf("name")
        && joinFlgByCustIdDtf("src_certtype") === ptyCustFlgDtf("cert_type_cd")
        && joinFlgByCustIdDtf("src_cert_no") === ptyCustFlgDtf("cert_no"),
      "left")
      .withColumnRenamed("cust_id", "flg_cust_id")
      .drop("name").drop("cert_no").drop("cert_type_cd");
    logger.info("关联3: {}", joinFlgWithFlg.show(300));

    return joinFlgWithFlg;
  }

  //  根据客户类型来分配客户号
  def getNewCustId(cust_type_cd: String): String = {
    var incr = hbaseDao.singleIncreament(":" + cust_type_cd + "_SEQ", cust_type_cd, ProManager.hbaseColumnFamily, "id", 1);
    var seq = String.format("%08d", Long.box(incr % 100000000));
    return cust_type_cd + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + seq;
  }

  def toSrcCustId(custId: String, srcCustId: String, srcSys: String, cust_type_cd: String): PTY_SRC_CUST_IDEntity = {
    val ptySrcCustIdEntity = new PTY_SRC_CUST_IDEntity;
    ptySrcCustIdEntity.setRowkey("");
    ptySrcCustIdEntity.setRowkey(custId.reverse + EncryptUtils.md5Encrypt32(srcCustId + srcSys));
    ptySrcCustIdEntity.setCust_id(custId);
    ptySrcCustIdEntity.setSrc_cust_id(srcCustId);
    ptySrcCustIdEntity.setSrc_sys(srcSys);
    ptySrcCustIdEntity.setCust_type_cd(cust_type_cd);
    ptySrcCustIdEntity.setData_crt_tm(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    return ptySrcCustIdEntity;
  }

  def toCustFlg(custId: String,
                certNo: String,
                cert_type_cd: String,
                name: String,
                cust_type_cd: String): PTY_CUST_FLGEntity = {
    val ptyCustFlgEntity = new PTY_CUST_FLGEntity;
    ptyCustFlgEntity.setRowkey(custId.reverse + EncryptUtils.md5Encrypt32(name + certNo + cert_type_cd));
    ptyCustFlgEntity.setCust_id(custId);
    ptyCustFlgEntity.setCust_type_cd(cust_type_cd);
    ptyCustFlgEntity.setCert_no(certNo);
    ptyCustFlgEntity.setCert_type_cd(cert_type_cd);
    ptyCustFlgEntity.setName(name);
    ptyCustFlgEntity.setData_crt_tm(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    return ptyCustFlgEntity;
  }

  def toCustGroup(custId: String, srcSys: String, srcCustId: String, groupId: String, src_cust_status_bf: String): PTY_CUST_GROUPEntity = {
    val ptyCustGroupEntity = new PTY_CUST_GROUPEntity
    ptyCustGroupEntity.setRowkey(custId.reverse + EncryptUtils.md5Encrypt32(srcCustId + groupId));
    ptyCustGroupEntity.setSrc_cust_id(srcCustId);
    ptyCustGroupEntity.setGroup_id(groupId);
    ptyCustGroupEntity.setCust_id(custId);
    ptyCustGroupEntity.setSrc_sys(srcSys);
    ptyCustGroupEntity.setSrc_cust_status_bf(src_cust_status_bf);
    ptyCustGroupEntity.setData_crt_tm(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    return ptyCustGroupEntity;
  }

  //  将证件不合法的客户类型转换成对应的临时客户类型
  def transCustType(custType: String, isFlgLegal: Boolean): String = {
    if (!CustTypeEntity.checkExists(custType)) {
      return custType;
    }
    if (!isFlgLegal) {
      val cust: CustTypeEntity.Value = CustTypeEntity.withName(custType);
      cust match {
        case CustTypeEntity.Indi => return CustTypeEntity.TempIndi.toString;
        case CustTypeEntity.Corp => return CustTypeEntity.TempCorp.toString;
        case CustTypeEntity.Prod => return CustTypeEntity.TempProd.toString;
        case _ => return custType;
      }
    }
    return custType;
  }
}

