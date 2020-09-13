package entity

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Author:BYDylan
 * Date:2020/8/21
 * Description:
 */
class RelationFieldEntity {
  var main_id: String = _;
  var group_id: String = _;
  var main_cust_id: String = _;
  var cust_ids: ArrayBuffer[String] = _;
  var fieldMap: HashMap[String, String] = _;
}
