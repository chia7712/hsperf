package com.chia7712.myscala

import org.apache.hadoop.hbase.util.Bytes

/**
  * A wrap to HBase Bytes util
  */
object ByteUtil {
  def toBytes(v:Int) = Bytes.toBytes(v)
  def toBytes(v:Long) = Bytes.toBytes(v)
  def toBytes(v:String) = Bytes.toBytes(v)
  def toBytes(v:Float) = Bytes.toBytes(v)
  def toBytes(v:Double) = Bytes.toBytes(v)
  def toString(v:Array[Byte]) = Bytes.toStringBinary(v)
  def compare:(Array[Byte], Array[Byte]) => Int = Bytes.compareTo
}
