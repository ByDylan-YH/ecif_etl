package main

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import annotation.HBaseTable
import entity.hbase.PTY_SRC_CUST_IDEntity
import org.apache.hadoop.hbase.client.Put

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}


/**
 * Author:BYDylan
 * Date:2020/9/7
 * Description:
 */
object Test {
  def main(args: Array[String]) {
    println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
    val entity = new PTY_SRC_CUST_IDEntity
    val arrayBuffer = new ArrayBuffer[String]()

    delete(classOf[PTY_SRC_CUST_IDEntity],arrayBuffer)
    //    println(getORMTable(entity))
    //    for (i <- 0 to 3) {
    //      breakable {
    //        if (i == 1) break()
    //        println(i)
    //      }
    //    }
    //    println("----------------------------")
    //    for (i <- 0 to 3) {
    //      println(i)
    //    }
  }

  def delete[T](t: Class[T],rowkeys:ArrayBuffer[String]): Unit = {

    var tableName: String = getORMTable(t.newInstance());
    println("tableName: " + tableName)
  }

  def getORMTable[T](t: T): String = {
    val table: HBaseTable = t.getClass.getAnnotation(classOf[HBaseTable]);
    println(table)
    //    val table: HBaseTable = clz.getClass.getAnnotation(classOf[HBaseTable]).asInstanceOf[HBaseTable];
    return table.tableName;
  }

}
