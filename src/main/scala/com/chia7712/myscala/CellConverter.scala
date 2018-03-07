package com.chia7712.myscala

import org.apache.hadoop.hbase.CellBuilderFactory
import org.apache.hadoop.hbase.CellBuilderType
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.{Cell => HCell}

/**
  * DON'T make the methods be implicit since the conversion is expensive
  */
object CellConverter {
  def toPutCell(c:Cell):HCell = {
    CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
      .setRow(c.key.row)
      .setFamily(c.key.family)
      .setQualifier(c.key.qualifier)
      .setType(org.apache.hadoop.hbase.Cell.Type.Put)
      .setValue(c.value)
      .build()
  }
  def toDeleteCell(key:Key):HCell = {
    CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
      .setRow(key.row)
      .setFamily(key.family)
      .setQualifier(key.qualifier)
      .setType(org.apache.hadoop.hbase.Cell.Type.Delete)
      .build()
  }
  def toPut(cell:Cell):Put = {
    val hcell = toPutCell(cell)
    new Put(hcell.getRowArray, true).add(hcell)
  }
  def toDelete(key:Key):Delete = {
    val hcell = toDeleteCell(key)
    new Delete(hcell.getRowArray, hcell.getRowOffset, hcell.getRowLength).add(hcell)
  }
  def toKey(c:HCell):Key = {
    Key(CellUtil.cloneRow(c), CellUtil.cloneFamily(c), CellUtil.cloneQualifier(c))
  }
  def toCell(c:HCell):Cell = {
    Cell(toKey(c), CellUtil.cloneValue(c))
  }
}
