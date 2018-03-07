package com.chia7712.hsperf

import scala.annotation.switch

trait Key extends Ordered[Key] {
  def row:Array[Byte]
  def family:Array[Byte]
  def qualifier:Array[Byte]
  override def equals(obj: scala.Any) = {
    obj match {
      case that:Key => compare(that) == 0
      case _ => false
    }
  }
  override def compare(that:Key) = Key.compare(this, that)
  override def toString = ByteConverter.toString(row) + "/" + ByteConverter.toString(family) + "/" + ByteConverter.toString(qualifier)
}

object Key {

  def compareRow(lhs:Key, rhs:Key) = ByteConverter.compare(lhs.row, rhs.row)
  def compareFamily(lhs:Key, rhs:Key) = ByteConverter.compare(lhs.family, rhs.family)
  def compareQualifier(lhs:Key, rhs:Key) = ByteConverter.compare(lhs.qualifier, rhs.qualifier)
  def compareColumn(lhs:Key, rhs:Key) = (compareFamily(lhs, rhs): @switch) match {
    case 0 => compareQualifier(lhs, rhs)
    case default => default
  }
  private def compare(lhs:Key, rhs:Key):Int = {
    (compareRow(lhs, rhs): @switch) match {
      case 0 => compareColumn(lhs, rhs)
      case default => default
    }
  }
  def apply(r:Array[Byte], f:Array[Byte]):Key = apply(r, f, Array.emptyByteArray)
  def apply(r:Array[Byte], f:Array[Byte], q:Array[Byte]) = {
    new Key() {
      override def row = r
      override def family = f
      override def qualifier = q
    }
  }
}
