package main

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions

/**
 * Author:BYDylan
 * Date:2020/10/21
 * Description:
 * sqoop import --connect jdbc:mysql://192.168.1.6:3306/ecifdb \
 * --driver com.mysql.cj.jdbc.Driver --username root --password By9216446o6 \
 * --table t_ods_ecif_pty_cert_hv \
 * --fields-terminated-by '|' \
 * --hive-import --hive-overwrite \
 * --hive-database ods_ecif --hive-table t_ods_ecif_pty_cert_hv \
 * --m 1
 */
object TestHive {
  System.setProperty("HADOOP_USER_NAME", "root");

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(s"ecifSpark");
    sparkConf.set("spark.shuffle.consolidateFiles", "true");
    sparkConf.set("spark.testing.memory", "8294960000");
    sparkConf.set("spark.ui.showConsoleProgress", "true");
    sparkConf.setMaster("local[*]");
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
    val sc = sparkSession.sparkContext;
    println("源表: ")
    //    hiveContext.sql("select group_id,cust_id,cn_name,cust_type_cd,cert_type_cd,cert_no,part_init_date from ods_ecif.t_ods_ecif_pty_cert_hv where part_init_date = '20191201'")
    //      .show(100)
    println("1.取出目标用户: ")
    val targetData: DataFrame = sparkSession.sql("select group_id,cust_id,cert_type_cd,cert_no,1 as init from ods_ecif.t_ods_ecif_pty_cert_hv where cust_type_cd = '02'")
    val ztData: DataFrame = sparkSession.sql("select regno,credit_code from ods_zt.t_ods_zt_enterprisebaseinfocollect_hv");
    //    targetData.show(100);
    //    ztData.show(100);
    println("2.关联政通: ")
    val ztResult: DataFrame = targetData.filter("cert_type_cd = '2010'").join(ztData, targetData("cert_no") === ztData("regno"))
      .unionAll(targetData.filter("cert_type_cd = '2020'")
        .join(ztData
          , targetData("cert_no").substr(1, 8) === ztData("credit_code").substr(9, 8)
            && targetData("cert_no").substr(10, 1) === ztData("credit_code").substr(17, 1)))
      .unionAll(targetData.filter("cert_type_cd = '2810'").join(ztData, targetData("cert_no") === ztData("credit_code")))
      .drop("regno").drop("credit_code")
      //      .drop("cert_no")
      .orderBy("group_id")
    //    ztResult.show(100)

    targetData.as("t1").join(ztResult.as("t2")
      , targetData("group_id") === ztResult("group_id")
        && targetData("cert_type_cd") === ztResult("cert_type_cd")
      , "left"
    )
      .withColumn("is_match_zt", functions.when(col("t2.group_id").isNotNull, 1).when(col("t2.group_id").isNull, 0))
      .select("t1.group_id", "t1.cert_no", "t1.cert_type_cd", "t1.cust_id", "t1.init", "is_match_zt")
      .orderBy(targetData("group_id"))
      .show(100)

    sc.stop();
  }
}
