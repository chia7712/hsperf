package com.chia7712.myscala


trait Table extends Closeable {
  def put(cells:Seq[Cell])
  def delete(key:Seq[Key])
  def deleteRow(row:Seq[Array[Byte]])
  def deleteFamily(rowFm:Seq[(Array[Byte], Array[Byte])])
  def tableName:String
}

