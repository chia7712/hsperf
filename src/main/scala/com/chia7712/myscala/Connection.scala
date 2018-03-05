package com.chia7712.myscala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, Delete, Put, TableDescriptorBuilder}
import org.apache.hadoop.hbase.{CellBuilderFactory, CellBuilderType, CellUtil, HBaseConfiguration, TableName}

import scala.collection.JavaConverters._

class Connection(config:Configuration) {
  private[this] val connection = ConnectionFactory.createConnection(config)
  def getTable(name:String) = {
    val tableName = TableName.valueOf(name)
    val table = connection.getTable(tableName)
    new Table() {
      override def put(cells: Cell*) = {
        val toHBaseCell = (c:Cell) => {
          CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setRow(c.key.row)
            .setFamily(c.key.family)
            .setQualifier(c.key.qualifier)
            .setType(org.apache.hadoop.hbase.Cell.Type.Put)
            .setValue(c.value)
            .build()
        }
        table.put(cells.map(toHBaseCell)
          .map(c => new Put(CellUtil.cloneRow(c), true).add(c)).asJava)
      }

      override def delete(key: Key*) = {
        val toHBaseCell = (key:Key) => {
          CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setRow(key.row)
            .setFamily(key.family)
            .setQualifier(key.qualifier)
            .setType(org.apache.hadoop.hbase.Cell.Type.Delete)
            .build()
        }
        table.delete(key.map(toHBaseCell)
          .map(c => new Delete(CellUtil.cloneRow(c)).add(c)).asJava)
      }

      override def deleteRow(row:Array[Byte]*) = {
        table.delete(row.map(new Delete(_)).asJava)
      }

      override def deleteFamily(rowFm: (Array[Byte], Array[Byte])*) = {
        table.delete(rowFm.map(rf => new Delete(rf._1).addFamily(rf._2)).asJava)
      }

      override def close() = table.close()

      override def tableName: String = name.toString
    }
  }

  def recreateTable(name:String, cfs:Array[Byte]*) = {
    val tableName = TableName.valueOf(name)
    val admin = connection.getAdmin
    try {
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }
      val builder = TableDescriptorBuilder.newBuilder(tableName)
      cfs.map(ColumnFamilyDescriptorBuilder.of).foreach(builder.addColumnFamily)
      admin.createTable(builder.build())
    } finally {
      admin.close()
    }
  }
  def close() = connection.close()
}
object Connection {
  def apply(config: Configuration = HBaseConfiguration.create()) = new Connection(config)
}
