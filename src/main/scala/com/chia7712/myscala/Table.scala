package com.chia7712.myscala



trait Table {
  def put(cells:Cell*)
  def delete(key:Key*)
  def deleteRow(row:Array[Byte]*)
  def deleteFamily(rowFm:(Array[Byte], Array[Byte])*)
  def tableName:String
  def close()
}

