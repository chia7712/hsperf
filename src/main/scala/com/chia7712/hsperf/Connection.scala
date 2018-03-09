package com.chia7712.hsperf

import com.chia7712.hsperf.CloseableUtil._
import com.chia7712.hsperf.KeyValueUtil._
import java.io.Closeable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import scala.collection.JavaConverters._

class Connection(config:Configuration) extends Closeable {
  private[this] val connection = ConnectionFactory.createConnection(config)
  def getTable(name:String, observer:TableObserver = TableObserver.DUMMY) = {
    val table = connection.getTable(TableName.valueOf(name))
    new Table() {

      override def putCells(cells: Seq[KeyValue]) = {
        try {
          table.put(cells.map(toPut).asJava)
        } finally {
          observer.postPutCells(cells)
        }
      }

      override def deleteCells(keys: Seq[Key]) = {
        try {
          table.delete(keys.map(toDelete).asJava)
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
    doClose(connection.getAdmin()) {
      admin => admin.getDescriptor(TableName.valueOf(tableName)).getColumnFamilies.map(_.getName)
    }
  }
  def close() = connection.close()
}
object Connection {
  def apply(config: Configuration = HBaseConfiguration.create()) = new Connection(config)
}

