package com.chia7712.myscala

import com.chia7712.myscala.Closeable._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.CellBuilderFactory
import org.apache.hadoop.hbase.CellBuilderType
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import scala.collection.JavaConverters._

class Connection(config:Configuration) extends Closeable {
  private[this] val connection = ConnectionFactory.createConnection(config)
  def getTable(name:String, observer:TableObserver = new TableObserver(){}) = {
    val table = connection.getTable(TableName.valueOf(name))
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

      override def putCells(cells: Seq[Cell]) = {
        try {
          table.put(cells.map(toCell)
            .map(c => new Put(CellUtil.cloneRow(c), true).add(c)).asJava)
        } finally {
          observer.postPutCells(cells)
        }

      }

      override def deleteCells(keys: Seq[Key]) = {
        try {
          table.delete(keys.map(toCell)
            .map(c => new Delete(CellUtil.cloneRow(c)).add(c)).asJava)
        } finally {
          observer.postDeleteCells(keys)
        }
      }

      override def deleteRows(rows:Seq[Array[Byte]]) = {
        try {
          table.delete(rows.map(new Delete(_)).asJava)
        } finally {
          observer.postDeleteRows(rows)
        }
      }

      override def deleteFamilies(rowFms:Seq[(Array[Byte], Array[Byte])]) = {
        try {
          table.delete(rowFms.map(rf => new Delete(rf._1).addFamily(rf._2)).asJava)
        } finally {
          observer.postDeleteFamilies(rowFms)
        }
      }

      override def close() = table.close()

      override def tableName: String = name
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

