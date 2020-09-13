package utils

/**
 * Author:BYDylan
 * Date:2020/8/29
 * Description:
 */
object IDCardUtils {
  private val rights = Array[Int](7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2, 1);
  private val checkDigits = Array[String]("1", "0", "X", "9", "8", "7", "6", "5", "4", "3", "2");

  private def getCheck_digit18(IDNumber: String) = {
    var sum = 0;
    var y = 0;
    y = 0;
    while (y <= 16) {
      val num = IDNumber.substring(y, y + 1).toInt;
      val right = rights(y);
      sum += num * right;
      y += 1;
    }
    y = sum % 11;
    checkDigits(y);
  }

  private def verify15(IDNumber: String) = {
    if (IDNumber.matches("^[1-9][0-9]{5}[0-9]{2}((01|03|05|07|08|10|12)(0[1-9]|[1-2][0-9]|3[0-1])|(04|06|09|11)(0[1-9]|[1-2][0-9]|30)|02(0[1-9]|[1-2][0-9]))[0-9]{3}$")) {
      true;
    } else {
      IDNumber.matches("^[1-9][0-9]{5}[0-9]{2}((01|03|05|07|08|10|12)(0[1-9]|[1-2][0-9]|3[0-1])|(04|06|09|11)(0[1-9]|[1-2][0-9]|30)|02(0[1-9]|1[0-9]|2[0-8]))[0-9]{3}$");
    }
  }

  private def verify18(IDNumber: String) = {
    if (IDNumber.matches("^[1-9][0-7]\\d{4}((([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})(((0[13578]|1[02])(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)(0[1-9]|[12][0-9]|30))|(02(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))0229))\\d{3}(\\d|X)?$")) {
      IDNumber.substring(17).equalsIgnoreCase(getCheck_digit18(IDNumber));
    }
    else {
      false;
    }
  }

  def isValidIDCard(IDNumber: String): Boolean = {
    if (IDNumber == null) false;
    else IDNumber.length match {
      case 15 =>
        verify15(IDNumber);
      case 18 =>
        verify18(IDNumber);
      case _ =>
        false;
    }
  }

  def getBirthday(IDNumber: String): String = {
    var dateString = "";
    var year = "";
    var month = "";
    var day = "";
    if (IDNumber.length == 15) {
      dateString = IDNumber.substring(6, 12);
      year = "19" + dateString.substring(0, 2);
      month = dateString.substring(2, 4);
      day = dateString.substring(4, 6);
    }
    else {
      dateString = IDNumber.substring(6, 14);
      year = dateString.substring(0, 4);
      month = dateString.substring(4, 6);
      day = dateString.substring(6, 8);
    }
    return year + "-" + month + "-" + day;
  }
}
