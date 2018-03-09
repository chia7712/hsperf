package com.chia7712.hsperf

import java.io.Closeable

trait Table extends Closeable {
  def putCells(cells:Seq[KeyValue])
  def deleteCells(keys:Seq[Key])
  def deleteRows(rows:Seq[Array[Byte]])
  def deleteFamilies(rowFms:Seq[(Array[Byte], Array[Byte])])
  def tableName:String
}

