package entity

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description:
 */
object CustTypeEntity extends Enumeration {
  type CustType = Value;
  val Indi = Value("01"); //正式个人客户
  val Corp = Value("02"); //正式公司客户
  val Prod = Value("05"); //产品户-正式(三要素齐全)
  val TempIndi = Value("03"); //个人临时客户
  val TempCorp = Value("04"); //公司临时客户
  val TempProd = Value("07"); //临时产品户(三要素不齐全)
  val IpoProd = Value("06"); //产品户私募客户
  val IpoTempProd = Value("08"); //临时产品私募客户

  //  检测是否存在此枚举值
  def checkExists(custType: String) = this.values.exists(_.toString == custType);

  //  是否是正式客户
  def checkIsValidCust(custType: String): Boolean = {
    if (!checkExists(custType)) {
      return false;
    }
    val items = Array(Indi, Corp, Prod, IpoProd);
    for (item <- items) {
      if (item.toString == custType) {
        return true;
      }
    }
    return false;
  }
}
