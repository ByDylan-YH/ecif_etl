package dao.impl

import java.io.IOException
import java.lang.reflect.Field
import java.util

import annotation.{EnumStoreType, HBaseColumn, HBaseTable}
import dao.HBaseDao
import manager.HBaseJDBCManager
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}
import utils.BytesUtils

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
 * Author:BYDylan
 * Date:2020/8/30
 * Description:
 */
class HBaseDaoImpl extends HBaseDao with Serializable {


  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass);
  var prefix: String = null;

  def this(etlDate: String) {
    this
    this.prefix = "ecifdb" + etlDate;
  }

  private def getORMTable[T](t: T): String = {
    val table: HBaseTable = t.getClass.getAnnotation(classOf[HBaseTable]);
    //    val table: HBaseTable = clz.getClass.getAnnotation(classOf[HBaseTable]).asInstanceOf[HBaseTable];
    return prefix + table.tableName;
  }

  override def save[T](objs: ArrayBuffer[T]): Boolean = {
    if (objs.size <= 0) return false;
    val tableName: String = getORMTable(objs(0));
    val columnFamilyNames: ArrayBuffer[String] = getHbaseColumn(objs(0));
    val admin: HBaseAdmin = HBaseJDBCManager.getConnection.getAdmin.asInstanceOf[HBaseAdmin];
    //    表不存在
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      createTable(tableName, columnFamilyNames);
    }
    val puts: ArrayBuffer[Put] = ArrayBuffer[Put]();
    for (obj <- objs) {
      breakable {
        if (obj == null) break();
        val put: Put = beanToPut(obj);
        puts += (put);
      }
    }
    return savePuts(puts, tableName);
  }

  def getHbaseColumn(entity: Any): ArrayBuffer[String] = {
    val hbaseColumnFamilyList: ArrayBuffer[String] = ArrayBuffer[String]();
    val fields: Array[Field] = entity.getClass.getDeclaredFields;
    for (f <- fields) { //获取字段中包含fieldMeta的注解
      val hbaseColumn: HBaseColumn = f.getAnnotation(classOf[HBaseColumn]);
      if (hbaseColumn != null) if (!hbaseColumnFamilyList.contains(hbaseColumn.family())) hbaseColumnFamilyList.+=(hbaseColumn.family());
    }
    return hbaseColumnFamilyList;
  }

  def parseObjRowKey[T](obj: T): Array[Byte] = {
    val field: Field = obj.getClass.getDeclaredField("rowkey");
    field.setAccessible(true);
    val value: AnyRef = field.get(obj);
    val rowKeyString: String = value.toString;
    val hbaseColumn: HBaseColumn = field.getAnnotation(classOf[HBaseColumn]);
    val columnType: EnumStoreType = hbaseColumn.columnType;
    if (columnType.equals(EnumStoreType.EST_STRING)) return Bytes.toBytes(rowKeyString);
    else if (columnType.equals(EnumStoreType.EST_BYTE)) return BytesUtils.hexStr2HexByte(rowKeyString);
    return null;
  }

  def beanToPut[T](obj: T): Put = {
    val put = new Put(parseObjRowKey(obj));
    val fields: Array[Field] = obj.getClass.getDeclaredFields;
    for (field <- fields) {
      breakable {
        field.setAccessible(true);
        val hbaseColumn: HBaseColumn = field.getAnnotation(classOf[HBaseColumn]);
        val family: String = hbaseColumn.family;
        val qualifier: String = hbaseColumn.qualifier;
        val columnType: EnumStoreType = hbaseColumn.columnType;
        if (StringUtils.isBlank(family) || StringUtils.isBlank(qualifier)) break();
        val fieldObj: Any = field.get(obj);
        if (fieldObj == null) break();
        if (fieldObj.getClass.isArray) logger.error("nonsupport");
        if (qualifier.equalsIgnoreCase("rowkey") || family.equalsIgnoreCase("rowkey")) {
          break();
        } else {
          if (field.get(obj) != null || StringUtils.isNotBlank(field.get(obj).toString)) {
            if (columnType.equals(EnumStoreType.EST_STRING)) {
              put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(field.get(obj).toString));
            } else if (columnType.equals(EnumStoreType.EST_BYTE)) {
              put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), BytesUtils.hexStr2HexByte(field.get(obj).toString));
            }
          }
        }
      }
    }
    return put;
  }

  override def createTable(tableName: String, familyColumns: ArrayBuffer[String]): Unit = {
    val hbaseTableName: TableName = TableName.valueOf(tableName);
    val admin: Admin = HBaseJDBCManager.getConnection.getAdmin;
    //    val nameDescriptor = new HTableDescriptor(hbaseTableName);
    val tableNameBuilder: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(hbaseTableName)
    try {
      for (familyColumn <- familyColumns) {
        //        val columnDescriptor = new HColumnDescriptor(familyColumn);
        val familyColumnDescriptor: ColumnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of(familyColumn);
        tableNameBuilder.setColumnFamily(familyColumnDescriptor);
        //        columnDescriptor.setMaxVersions(1);
        //        nameDescriptor.addFamily(columnDescriptor);
      }
      admin.createTable(tableNameBuilder.build());
    } catch {
      case e: IOException =>
        logger.error("Create table: " + tableName + " exception: {}", e.getMessage);
    } finally if (admin != null) admin.close();
  }

  override def savePuts(puts: ArrayBuffer[Put], tableName: String): Boolean = {
    val writeBufferSize: Long = 10 * 1024 * 1024;
    if (StringUtils.isBlank(tableName)) return false;
    try {
      val params: BufferedMutatorParams = new BufferedMutatorParams(TableName.valueOf(tableName)).listener((e: RetriesExhaustedWithDetailsException, mutator: BufferedMutator) => {
        def foo(e: RetriesExhaustedWithDetailsException, mutator: BufferedMutator) = {
          for (i <- 0 until e.getNumExceptions) {
            logger.error("Failed to sent put: {}", e.getRow(i));
          }
        }
      }).writeBufferSize(writeBufferSize);
      val mutator: BufferedMutator = HBaseJDBCManager.getConnection.getBufferedMutator(params);
      try {
        val buffer: ArrayBuffer[Put] = ArrayBuffer[Put]();
        var bufferSize = 0;
        val putsSize: Int = puts.size;
        for (i <- 0 until putsSize) {
          val put: Put = puts(i);
          buffer.+=(put);
          bufferSize += put.heapSize.toInt;
          if (bufferSize >= writeBufferSize || i == putsSize - 1) {
            mutator.mutate(JavaConverters.bufferAsJavaList(buffer));
            mutator.flush();
            buffer.clear;
            bufferSize = 0;
          }
        }
      } finally if (mutator != null) mutator.close();
      return true;
    } catch {
      case e: IOException =>
        logger.error("Save in table:" + tableName + " exception: {}", e.getMessage);
        return false;
    }
  }

  override def delete[T](t: Class[T], rowkeys: ArrayBuffer[String]): Unit = {
    var tableName: String = getORMTable(t.newInstance());
    if (StringUtils.isBlank(tableName)) return
    val deletes = new ArrayBuffer[Delete]();
    for (rowKey <- rowkeys) {
      //      if (StringUtils.isBlank(rowKey)) break()
      deletes += (new Delete(Bytes.toBytes(rowKey)))
    }
    //    delete(deletes, tableName);
    val table: Table = HBaseJDBCManager.getConnection.getTable(TableName.valueOf(tableName));
    try {
      if (StringUtils.isBlank(tableName)) return
      table.delete(JavaConverters.bufferAsJavaList(deletes));
    } catch {
      case e: IOException =>
        logger.error("Delete in table:" + tableName + " error: {}", e.getMessage);
    } finally if (table != null) table.close();
  }

  override def delete(deletes: ArrayBuffer[Delete], tableName: String): Unit = {
    val table: Table = HBaseJDBCManager.getConnection.getTable(TableName.valueOf(tableName))
    try {
      if (StringUtils.isBlank(tableName)) return
      table.delete(JavaConverters.bufferAsJavaList(deletes));
    } catch {
      case e: IOException =>
        logger.error("Delete in table:" + tableName + " error: {}", e.getMessage);
    } finally if (table != null) table.close();
  }

  override def scaneByPrefixFilter(tableName: String, rowPrifix: String): util.Iterator[Result] = {
    try {
      val table: Table = HBaseJDBCManager.getConnection.getTable(TableName.valueOf(tableName));
      val scan: Scan = new Scan().setRowPrefixFilter(rowPrifix.getBytes());
      val rs: ResultScanner = table.getScanner(scan);
      return rs.iterator;
    } catch {
      case exception: Exception =>
        logger.error("scan data failed: {}", exception.getMessage);
        return new util.ArrayList[Result]().iterator;
    }
  }

  override def singleIncreament(tableName: String, rowKey: String, columnFamily: String, column: String, amount: Long): Long = {
    var increment = 0L;
    val table: Table = HBaseJDBCManager.getConnection.getTable(TableName.valueOf(prefix + tableName));
    try {
      increment = table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamily), Bytes.toBytes(column), amount);
    } catch {
      case e: Exception =>
        logger.error("singleIncreament failed: {}", e.getMessage);
    } finally if (table != null) table.close();
    return increment;
  }

}

