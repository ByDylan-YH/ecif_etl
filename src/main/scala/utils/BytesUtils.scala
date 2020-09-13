package utils

import java.util
import java.util.regex.{Matcher, Pattern}

import org.apache.commons.codec.binary.Hex
import org.apache.commons.lang3.math.NumberUtils
import org.apache.commons.lang3.{ArrayUtils, StringUtils}

import scala.collection.mutable


/**
 * Author:BYDylan
 * Date:2020/8/24
 * Description:
 */
object BytesUtils {

  //高位在前，低位在后
  def int2bytes(num: Int): Array[Byte] = {
    val result = new Array[Byte](4);
    result(0) = ((num >>> 24) & 0xff).toByte;
    result(1) = ((num >>> 16) & 0xff).toByte;
    result(2) = ((num >>> 8) & 0xff).toByte;
    result(3) = ((num >>> 0) & 0xff).toByte;
    return result;
  }

  //高位在前，低位在后
  def bytes2int(bytes: Array[Byte]): Int = {
    var result = 0;
    if (bytes.length == 4) {
      val a: Int = (bytes(0) & 0xff) << 24;
      val b: Int = (bytes(1) & 0xff) << 16;
      val c: Int = (bytes(2) & 0xff) << 8;
      val d: Int = bytes(3) & 0xff;
      result = a | b | c | d;
    }
    return result;
  }

  def short2bytes(num: Int): Array[Byte] = {
    val result = new Array[Byte](2);
    val shortNum: Short = (num & 0xffff).toShort;
    result(0) = ((shortNum >>> 8) & 0xff).toByte;
    result(1) = ((shortNum >>> 0) & 0xff).toByte;
    return result;
  }


  def bytes2short(bytes: Array[Byte]): Int = {
    var result = 0;
    if (bytes.length == 2) {
      val high: Int = (bytes(0) & 0xff) << 8;
      val low: Int = bytes(1) & 0xff;
      result = high | low;
    }
    return result;
  }


  def hexStr2HexByte(strSource: String): Array[Byte] = {
    var newStrSource: String = strSource;
    var len: Int = strSource.length;
    val mod: Int = len % 2;
    if (mod != 0) {
      newStrSource = "0".concat(strSource);
      len = strSource.length;
    }
    var abt = new Array[Byte](len);
    if (len >= 2) len = len / 2;
    val bbt = new Array[Byte](len);
    abt = newStrSource.getBytes;
    var j = 0;
    var k = 0;
    for (p <- 0 until newStrSource.length / 2) {
      if ((abt(2 * p) >= '0') && (abt(2 * p) <= '9')) j = abt(2 * p) - '0';
      else if ((abt(2 * p) >= 'a') && (abt(2 * p) <= 'z')) j = abt(2 * p) - 'a' + 0x0a;
      else j = abt(2 * p) - 'A' + 0x0a;
      if ((abt(2 * p + 1) >= '0') && (abt(2 * p + 1) <= '9')) k = abt(2 * p + 1) - '0';
      else if ((abt(2 * p + 1) >= 'a') && (abt(2 * p + 1) <= 'z')) k = abt(2 * p + 1) - 'a' + 0x0a;
      else k = abt(2 * p + 1) - 'A' + 0x0a;
      val a: Int = (j << 4) + k;
      val b: Byte = a.toByte;
      bbt(p) = b;
    }
    return bbt;
  }

  def hexByte2HexStr(bytes: Array[Byte]): String = {
    if (ArrayUtils.isEmpty(bytes)) return ""
    //        StringBuffer temp = new StringBuffer(bytes.length * 2);
    //        for (int i = 0; i < bytes.length; i++)
    //            temp.append((byte) ((bytes[i] & 0xf0) >>> 4));
    //            temp.append((byte) (bytes[i] & 0x0f));
    //        return temp.toString().substring(0, 1).equalsIgnoreCase("0") ? temp
    //                .toString().substring(1) : temp.toString();
    //        return Arrays.stream(ArrayUtils.toObject(bytes))
    //                .map(e -> String.format("%02x", e.byteValue() & 0xff))
    //                .reduce((start, item) -> start + item)
    //                .get();
    return new String(Hex.encodeHex(bytes));
  }

  def bytes2Int(bytes: Array[Byte], startIdx: Int): Int = {
    if (bytes.length - startIdx < 4) return 0
    var value: Int = bytes({
      startIdx + 1;
      startIdx - 1;
    });
    value = (value << 8) + (bytes({
      startIdx + 1;
      startIdx - 1
    }) & 0xFF);
    value = (value << 8) + (bytes({
      startIdx + 1;
      startIdx - 1
    }) & 0xFF);
    value = (value << 8) + (bytes({
      startIdx + 1;
      startIdx - 1
    }) & 0xFF);
    return value & 0xFFFFFFFF;
  }

  def bitSet2ByteArray(bitSet: util.BitSet): Array[Byte] = {
    val bytes = new Array[Byte](bitSet.size / 8);
    var i = 0;
    while ( {
      i < bitSet.size;
    }) {
      val index: Int = i / 8;
      val offset: Int = 7 - i % 8;
      bytes(index).|(if (bitSet.get(i)) 1.toByte else 0.toByte) << offset;
      i += 1;
    }
    return bytes;
  }

  def byteArray2BitSet(bytes: Array[Byte]): util.BitSet = {
    val bitSet = new util.BitSet(bytes.length * 8);
    var index = 0;
    for (i <- 0 until bytes.length) {
      for (j <- 7 to 0 by -1) {
        bitSet.set({
          index += 1;
          index - 1;
        }, (bytes(i) & (1 << j)) >> j == 1);
      }
    }
    return bitSet;
  }

  def isHexString(a: String): Boolean = try {
    Integer.parseInt(a, 16);
    return true;
  } catch {
    case e: Exception =>
      return false;
  }

  /**
   * @param sourcebytes
   * @param oprString 操作字符串,如0:0:1,0:1:0
   * @return
   */
  def oprBytes(sourcebytes: Array[Byte], oprString: String): Array[Byte] = {
    if (StringUtils.isBlank(oprString)) return sourcebytes;
    val oprs: Array[String] = StringUtils.split(oprString, ",");
    val oprMap: mutable.HashMap[Integer, mutable.HashMap[Integer, Integer]] = mutable.HashMap[Integer, mutable.HashMap[Integer, Integer]]();
    util.Arrays.stream(oprs).forEach((e: String) => {
      def foo(e: String): Any = {
        val oneOprs: Array[String] = StringUtils.split(e, ":");
        if (oneOprs.length != 3) return;
        val byteIdx: Int = NumberUtils.toInt(oneOprs(0));
        val bitIdx: Int = NumberUtils.toInt(oneOprs(1));
        val oprValue: Int = NumberUtils.toInt(oneOprs(2));
        if (!oprMap.contains(byteIdx)) {
          val bitOprMap: mutable.HashMap[Integer, Integer] = mutable.HashMap[Integer, Integer]();
          oprMap.put(byteIdx, bitOprMap);
        }
        return oprMap.get(byteIdx).get.put(bitIdx, oprValue);
      }
    });
    val destBytes = new Array[Byte](sourcebytes.length);
    for (byteIdx <- 0 until sourcebytes.length) {
      destBytes(byteIdx) = sourcebytes(byteIdx);
      if (oprMap.contains(byteIdx)) {
        val bitOprMap: mutable.HashMap[Integer, Integer] = oprMap.get(byteIdx).get;
        for (bitIdx <- bitOprMap) {
          destBytes(byteIdx) = BitUtils.setBitValue(destBytes(byteIdx), bitIdx._1, bitOprMap.get(bitIdx._1).get);
        }
      }
    }
    return destBytes;
  }

  /**
   * 判断字符是否是中文
   *
   * @param c 字符
   * @return 是否是中文
   */
  def isChinese(c: Char): Boolean = {
    val ub: Character.UnicodeBlock = Character.UnicodeBlock.of(c)
    (ub eq Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS) || (ub eq Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS) || (ub eq Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A) || (ub eq Character.UnicodeBlock.GENERAL_PUNCTUATION) || (ub eq Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION) || (ub eq Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS)
  }

  /**
   * 判断字符串是否是乱码
   *
   * @param strName 字符串
   * @return 是否是乱码
   */
  def isMessyCode(strName: String): Boolean = {
    val pattern: Pattern = Pattern.compile("\\s*|t*|r*|n*");
    val matcher: Matcher = pattern.matcher(strName);
    val after: String = matcher.replaceAll("");
    val temp: String = after.replaceAll("\\p{P}", "");
    val ch: Array[Char] = temp.trim.toCharArray;
    val chLength: Float = ch.length;
    var count = 0;
    for (i <- 0 until ch.length) {
      val c: Char = ch(i);
      if (!Character.isLetterOrDigit(c)) if (!isChinese(c)) count = count + 1;
    }
    val result: Float = count / chLength;
    return result > 0.4;
  }
}
