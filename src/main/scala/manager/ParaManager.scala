package manager

import entity.TaskEnvEntity

import scala.collection.mutable

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description: SQL 参数修改
 */
object ParaManager {
  def process(sql: String, taskEnvEntity: TaskEnvEntity): String = {
    val codeMap: mutable.HashMap[String, String] = taskEnvEntity.asInstanceOf[TaskEnvEntity].codeBroadcast.value;
    val etlDate: String = taskEnvEntity.asInstanceOf[TaskEnvEntity].etlDate;
    val gr_type: collection.Set[String] = codeMap.keySet.map(r => {
      val strings = r.split("_");
      ((strings(0), strings(1)), strings(2));
    }).groupBy(_._1)(("3", "S008")).map(_._2);
    val gr: String = "'" + gr_type.mkString("','") + "'";
    val qy_type: collection.Set[String] = codeMap.keySet.map(r => {
      val strings = r.split("_");
      ((strings(0), strings(1)), strings(2));
    }).groupBy(_._1)(("4", "S008")).map(_._2);
    val qy: String = "'" + qy_type.mkString("','") + "'";
    //    组织机构代码
    val org_inst_cd: String = "'" + codeMap.filter(t => t._1.startsWith("4_S008_") && t._2 == "2020").keySet.toArray.apply(0).split("_")(2) + "'";
    //    营业执照
    val biz_license_no: String = "'" + codeMap.filter(t => t._1.startsWith("4_S008_") && t._2 == "2010").keySet.toArray.apply(0).split("_")(2) + "'";
    //    统一社会信用代码
    val un_soc_cr_cd: String = "'" + codeMap.filter(t => t._1.startsWith("4_S008_") && t._2 == "2810").keySet.toArray.apply(0).split("_")(2) + "'";
    //    税务登记证号
    val tax_no: String = "'" + codeMap.filter(t => t._1.startsWith("4_S008_") && t._2 == "2090").keySet.toArray.apply(0).split("_")(2) + "'";
    return sql.replace("${gr_type}", gr)
      .replace("${qy_type}", qy)
      .replace("${org_inst_cd}", org_inst_cd)
      .replace("${biz_license_no}", biz_license_no)
      .replace("${un_soc_cr_cd}", un_soc_cr_cd)
      .replace("${tax_no}", tax_no)
      .replace("${etlDate}", etlDate);
  }
}
