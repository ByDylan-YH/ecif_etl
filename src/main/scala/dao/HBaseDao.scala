package dao

import java.util

import org.apache.hadoop.hbase.client.{Delete, Put, Result}

import scala.collection.mutable.ArrayBuffer

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description:
 */
abstract class HBaseDao {
  def createTable(tableName: String, familyColumns: ArrayBuffer[String]): Unit;

  //  def getEtlDate():String;

  //  def save(objs: HBaseEntity*): Boolean;

  def save[T](objs: ArrayBuffer[T]): Boolean;

  //  def save(tableName: String, objs: HBaseEntity*): Boolean;

  def savePuts(puts: ArrayBuffer[Put], tableName: String): Boolean;

  //  def delete(clazz: Class[HBaseEntity], rowkeys: String*): Unit;

  def delete[T](t: Class[T], rowkeys: ArrayBuffer[String]): Unit;
  def delete(deletes: ArrayBuffer[Delete], tableName: String): Unit;

  def scaneByPrefixFilter(tableName: String, rowPrifix: String): util.Iterator[Result];

  def singleIncreament(tableName: String, rowKey: String, columnFamily: String, column: String, amount: Long): Long;
}
