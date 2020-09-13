package manager

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession
import utils.IDCardUtils

/**
 * Author:BYDylan
 * Date:2020/8/29
 * Description: 自定义 UDF 函数
 */
object UDFManager {
  private val standardFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  def register(sparkSession: SparkSession) = {
    sparkSession.udf.register("idConvert", idConvert);
    sparkSession.udf.register("transZeroToEmpty", transZeroValueToEmpty);
    sparkSession.udf.register("idlegal", idlegal);
  }

  //  旧身份证15位转换成新的18位
  val idConvert = (idKind: String, idType: String, idNumber: String) => {
    if (idKind == idType && idNumber.trim.length == 15) {
      var retId: String = "";
      var id17: String = "";
      var sum: Int = 0;
      var y: Int = 0;
      // 定义数组存放加权因子 weight factor
      val wf: Array[Int] = Array(7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2);
      // 定义数组存放校验码 check code
      val cc: Array[String] = Array("1", "0", "X", "9", "8", "7", "6", "5", "4", "3", "2");
      // 加上两位年19
      id17 = idNumber.substring(0, 6) + "19" + idNumber.substring(6);
      // 十七位数字本体码加权求和
      var i = 0;
      for (i <- 0 until 17) {
        sum = sum + Integer.valueOf(id17.substring(i, i + 1)) * wf(i);
      }
      // 计算模
      y = sum % 11;
      // 通过模得到对应的校验码 cc[y]
      retId = id17 + cc(y);
      retId;
    } else {
      idNumber;
    }
  }

  val transZeroValueToEmpty = (value: String) => {
    if (value == null || "0".equals(value) || "000000000000000".equals(value) || "000000000000000000".equals(value)) {
      ""
    } else {
      value;
    }
  }

  //判断身份证号是否合法
  val idlegal = (idKind: String, idType: String, idNumber: String) => {
    var currentDate = LocalDateTime.now().format(standardFormat);
//    1.先判断证件类型是否是身份证
    if (idKind == idType) {
//      2.判断身份证是否合法
      if (idNumber != null && IDCardUtils.isValidIDCard(idNumber.trim)) {
//        3.判断身份证日期是否大于系统当前日期
        if (IDCardUtils.getBirthday(idNumber.trim) <= currentDate) {
          "1";
        } else {
          "0";
        }
      } else {
        "0";
      }
    } else {
//      如果证件类型不是身份证,返回1,合法
      null;
    }
  }
}
