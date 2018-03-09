package com.chia7712.hsperf

trait KeyValue extends Ordered[KeyValue] {
  def key:Key
  def value:Array[Byte]
  override def equals(obj: scala.Any) = {
    obj match {
      case that:KeyValue => compare(that) == 0
      case _ => false
    }
  }
  override def compare(that:KeyValue) = key.compare(that.key)
  override def toString = key.toString + "/" + ByteUtil.toString(value)
}
object KeyValue {
  def apply(k:Key, v:Array[Byte]) = {
    new KeyValue() {
      override def key = k
      override def value = v
    }
  }
}