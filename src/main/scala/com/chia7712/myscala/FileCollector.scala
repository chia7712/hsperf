package com.chia7712.myscala

import java.io.File

object FileCollector {
  def apply(file:File): File = {
    file
  }
  def unapply(file:File): Option[(String, Long)] = {
    Some(file.getAbsolutePath, file.length())
  }

  def main(args:Array[String]): Unit = {
    var fc = FileCollector;
    var f = fc(new File("/home/chia7712/hbase"))
    println(f.getAbsolutePath)

    val Student(number, name, addr) = f
    println(number)
    println(name)
    println(addr)
    val FileCollector(path, size) = f
    println(path)
    println(size)

    f match {
      case FileCollector(path, size) => println("Hello file:" + size)
      case Student(name, path, parent) => println("Hello file:" + name)

    }
  }
}
object Student {
  def unapply(file: File): Option[(String, String, String)] = {
    Some(file.getName, file.getAbsolutePath, file.getParent)
  }
}
