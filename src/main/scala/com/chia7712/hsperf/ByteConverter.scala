package com.chia7712.hsperf

import org.apache.hadoop.hbase.util.Bytes

/**
  * A wrap to HBase Bytes util
  */
object ByteConverter {
  def toShort(v:Array[Byte]) = Bytes.toShort(v)
  def toInt(v:Array[Byte]) = Bytes.toInt(v)
  def toLong(v:Array[Byte]) = Bytes.toLong(v)
  def toFloat(v:Array[Byte]) = Bytes.toFloat(v)
  def toDouble(v:Array[Byte]) = Bytes.toDouble(v)
  def toString(v:Array[Byte]) = Bytes.toString(v)
  def toBytes(v:Short) = Bytes.toBytes(v)
  def toBytes(v:Int) = Bytes.toBytes(v)
  def toBytes(v:Long) = Bytes.toBytes(v)
  def toBytes(v:String) = Bytes.toBytes(v)
  def toBytes(v:Float) = Bytes.toBytes(v)
  def toBytes(v:Double) = Bytes.toBytes(v)
  def toStringBinary(v:Array[Byte]) = Bytes.toStringBinary(v)
  def compare:(Array[Byte], Array[Byte]) => Int = Bytes.compareTo
}
