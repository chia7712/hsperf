package com.chia7712.myscala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, Delete, Put, TableDescriptorBuilder}
import org.apache.hadoop.hbase.{CellBuilderFactory, CellBuilderType, CellUtil, TableName}

import scala.collection.JavaConverters._
import scala.annotation.switch

class Connection(config:Configuration) {
  private[this] val connection = ConnectionFactory.createConnection(config)
  def createTable(name:TableName) = {
    val table = connection.getTable(name)
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

      override def deleteRow(row: Array[Byte]*) = {
        table.delete(row.map(new Delete(_)).asJava)
      }

      override def deleteFamily(rowFm: (Array[Byte], Array[Byte])*) = {
        table.delete(rowFm.map(rf => new Delete(rf._1).addFamily(rf._2)).asJava)
      }

      override def close() = table.close()
    }
  }
  def recreateTable(name:TableName, cfs:String*) = {
    val admin = connection.getAdmin
    try {
      (admin.tableExists(name): @switch) match {
        case false => {
          admin.disableTable(name)
          admin.deleteTable(name)
        }
        case _ => {
          val builder = TableDescriptorBuilder.newBuilder(name)
          cfs.map(cf => ColumnFamilyDescriptorBuilder.of(cf))
             .foreach(builder.addColumnFamily)
          admin.createTable(builder.build())
        }
      }
    } finally {
      admin.close()
    }
  }
  def close() = connection.close()
}
