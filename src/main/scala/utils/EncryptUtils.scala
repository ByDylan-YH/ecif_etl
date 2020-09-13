package utils

import java.io.IOException
import java.security.{MessageDigest, NoSuchAlgorithmException, SecureRandom}

import javax.crypto.spec.SecretKeySpec
import javax.crypto.{Cipher, KeyGenerator}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import sun.misc.{BASE64Decoder, BASE64Encoder}

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description:
 */
object EncryptUtils {
  private val logger: Logger = LoggerFactory.getLogger(EncryptUtils.getClass);

  //  16位MD5加密
  def md5Encrypt16(input: String): String = {
    getMD5AsHex(input.getBytes()).substring(8, 24);
  }

  //  32位MD5加密
  def md5Encrypt32(input: String): String = {
    getMD5AsHex(input.getBytes());
  }

  private def getMD5AsHex(key: Array[Byte]): String = {
    val offset = 0;
    getMD5AsHex(key, offset, key.length);
  };

  private def getMD5AsHex(key: Array[Byte], offset: Int, length: Int): String = try {
    val md = MessageDigest.getInstance("MD5");
    md.update(key, offset, length);
    val digest = md.digest;
    new String(Hex.encodeHex(digest));
  } catch {
    case var5: NoSuchAlgorithmException =>
      throw new RuntimeException("Error computing MD5 hash", var5);
  }

  def base64Encode(bytes: Array[Byte]): String = new BASE64Encoder().encode(bytes);

  def base64Decode(base64Code: String): Array[Byte] = try
    if (StringUtils.isEmpty(base64Code))
      return null
    else new BASE64Decoder().decodeBuffer(base64Code)
  catch {
    case e: IOException =>
      logger.error("Failed to base64Decode: {}", e);
      return null
  }

  def aesEncryptToBytes(content: String, encryptKey: String): Array[Byte] = {
    var bytes: Array[Byte] = null
    try {
      val kgen = KeyGenerator.getInstance("AES");
      val random = SecureRandom.getInstance("SHA1PRNG");
      random.setSeed(encryptKey.getBytes);
      kgen.init(128, random);
      val cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(kgen.generateKey.getEncoded, "AES"));
      bytes = cipher.doFinal(content.getBytes("UTF-8"));
    } catch {
      case e: Exception =>
        logger.error("Failed to aesEncryptToBytes: {}", e);
    }
    return bytes;
  }

  @throws[Exception]
  def aesEncrypt(content: String, encryptKey: String): String = base64Encode(aesEncryptToBytes(content, encryptKey));


  def aesDecryptByBytes(encryptBytes: Array[Byte], decryptKey: String): String = {
    var decryptBytes = new Array[Byte](0);
    try {
      val kgen = KeyGenerator.getInstance("AES");
      val random = SecureRandom.getInstance("SHA1PRNG");
      random.setSeed(decryptKey.getBytes);
      kgen.init(128, random);
      val cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(kgen.generateKey.getEncoded, "AES"));
      decryptBytes = cipher.doFinal(encryptBytes);
    } catch {
      case e: Exception =>
        logger.error("Failed to aesEncryptToBytes: {}", e);
    }
    new String(decryptBytes)
  }

  def aesDecrypt(encryptStr: String, decryptKey: String): String = {
    if (StringUtils.isEmpty(encryptStr)) return null
    else return aesDecryptByBytes(base64Decode(encryptStr), decryptKey)
  }
}
