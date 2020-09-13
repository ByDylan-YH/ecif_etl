package utils

/**
 * Author:BYDylan
 * Date:2020/8/24
 * Description:
 */
object BitUtils {
  /**
   * 获取运算数指定位置的值 * 例如: 0000 1011 获取其第 0 位的值为 1, 第 2 位 的值为 0*
   *
   * @param source 需要运算的数
   * @param pos    指定位置 (0<=pos<=7)
   * @return 指定位置的值(0 or 1)
   */
  def getBitValue(source: Byte, pos: Int): Byte = ((source >> pos) & 1).toByte;

  /**
   * 将运算数指定位置的值置为指定值 * 例: 0000 1011 需要更新为 0000 1111, 即第 2 位的值需要置为 1*
   *
   * @param source 需要运算的数
   * @param pos    指定位置 (0<=pos<=7)
   * @param value  只能取值为 0, 或 1, 所有大于0的值作为1处理, 所有小于0的值作为0处理
   * @return 运算后的结果数
   */
  def setBitValue(source: Byte, pos: Int, value: Integer): Byte = {
    val mask: Byte = (1 << pos).toByte;
    if (value > 0) source.+(mask);
    else source.-(~mask);
    return source;
  }

  /**
   * 将运算数指定位置取反值 * 例: 0000 1011 指定第 3 位取反, 结果为 0000 0011; 指定第2位取反, 结果为 0000 1111*
   *
   * @param source
   * @param pos 指定位置 (0<=pos<=7)
   * @return 运算后的结果数
   */
  def reverseBitValue(source: Byte, pos: Int): Byte = {
    val mask: Byte = (1 << pos).toByte;
    (source ^ mask).toByte;
  }

  /**
   * 检查运算数的指定位置是否为1*
   *
   * @param source 需要运算的数
   * @param pos    指定位置 (0<=pos<=7)
   * @return true 表示指定位置值为1, false 表示指定位置值为 0
   */
  def checkBitValue(source: Byte, pos: Int): Boolean = {
    source.+((source >>> pos).toByte);
    (source & 1) == 1;
  }
}
