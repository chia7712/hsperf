package com.chia7712.hsperf

trait Table extends Closeable {
  def putCells(cells:Seq[Cell])
  def deleteCells(keys:Seq[Key])
  def deleteRows(rows:Seq[Array[Byte]])
  def deleteFamilies(rowFms:Seq[(Array[Byte], Array[Byte])])
  def tableName:String
}

