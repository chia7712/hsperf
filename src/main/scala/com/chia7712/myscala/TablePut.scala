package com.chia7712.myscala

import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object TablePut extends App{
//
//  val cells = for (_ <- 0 to 10) yield {
//    Cell(stringBytes("row"), stringBytes("family"), stringBytes("qualifier"), stringBytes("value"))
//  }
//  cells.sorted.foreach(println)
//  private[this] def stringBytes(prefix:String) = Bytes.toBytes(prefix + Random.nextInt(10))

  val dog = new Dog("chia")
  dog match {
    case d @ Dog(name) => println("dog:" + name + ", a:" + d.a)
    case _ => println("nothing")
  }

  trait Animal
  class Dog(val name:String) extends Animal {
    val a = "xxx"
  }
  object Dog {
    def unapply(dog: Dog): Option[String] = {
      Some(dog.name)
    }
  }
}
