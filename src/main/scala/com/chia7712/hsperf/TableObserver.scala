package com.chia7712.hsperf

trait TableObserver {
  def postPutCells(cells:Seq[Cell]):Unit = {}
  def postDeleteCells(key:Seq[Key]):Unit = {}
  def postDeleteRows(row:Seq[Array[Byte]]):Unit = {}
  def postDeleteFamilies(rowFm:Seq[(Array[Byte], Array[Byte])]):Unit = {}
}

object TableObserver {
  val DUMMY = new TableObserver(){}
}
