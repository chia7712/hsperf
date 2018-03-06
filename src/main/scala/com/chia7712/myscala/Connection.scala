package com.chia7712.myscala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptorBuilder, ConnectionFactory, Delete, Put, TableDescriptorBuilder}
import org.apache.hadoop.hbase.{CellBuilderFactory, CellBuilderType, CellUtil, HBaseConfiguration, TableName}

import scala.collection.JavaConverters._
import com.chia7712.myscala.Closeable._

import scala.util.Try
class Connection(config:Configuration) extends Closeable {
  private[this] val connection = ConnectionFactory.createConnection(config)
  def getTable(name:String) = {
    val tableName = TableName.valueOf(name)

    val table = connection.getTable(tableName)
    new Table() {
      def toCell(c:Cell) = {
        CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
          .setRow(c.key.row)
          .setFamily(c.key.family)
          .setQualifier(c.key.qualifier)
          .setType(org.apache.hadoop.hbase.Cell.Type.Put)
          .setValue(c.value)
          .build()
      }
      def toCell(key:Key) = {
        CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
          .setRow(key.row)
          .setFamily(key.family)
          .setQualifier(key.qualifier)
          .setType(org.apache.hadoop.hbase.Cell.Type.Delete)
          .build()
      }

      override def put(cells: Seq[Cell]) = {
        table.put(cells.map(toCell)
          .map(c => new Put(CellUtil.cloneRow(c), true).add(c)).asJava)
      }

      override def delete(key: Seq[Key]) = {
        table.delete(key.map(toCell)
          .map(c => new Delete(CellUtil.cloneRow(c)).add(c)).asJava)
      }

      override def deleteRow(row:Seq[Array[Byte]]) = {
        table.delete(row.map(new Delete(_)).asJava)
      }

      override def deleteFamily(rowFm:Seq[(Array[Byte], Array[Byte])]) = {
        table.delete(rowFm.map(rf => new Delete(rf._1).addFamily(rf._2)).asJava)
      }

      override def close() = table.close()

      override def tableName: String = name.toString
    }
  }

  def getColumns(tableName:String) = {
    doJavaClose(connection.getAdmin()) {
      admin => admin.getDescriptor(TableName.valueOf(tableName)).getColumnFamilies.map(_.getName)
    }
  }
  def close() = connection.close()
}
object Connection {
  def apply(config: Configuration = HBaseConfiguration.create()) = new Connection(config)
}

