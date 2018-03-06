package com.chia7712.myscala

import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait Closeable {
  def close():Unit
}
object Closeable {
  def doFinally[A, B](generator: => A)(worker: A => B)(closer: A => Unit) = {
    Try(generator) match {
      case Success(obj) => {
        try {
          worker(obj)
        } finally {
          closer(obj)
        }
      }
      case Failure(e) => throw e
    }
  }
  def doClose[A <: Closeable, B](generator: => A)(worker: A => B) = {
    Try(generator) match {
      case Success(obj) => {
        try {
          worker(obj)
        } finally {
          obj.close()
        }
      }
      case Failure(e) => throw e
    }
  }

  def doJavaClose[A <: java.io.Closeable, B](generator: => A) (worker: A => B) = {
    Try(generator) match {
      case Success(obj) => {
        try {
          worker(obj)
        } finally {
          obj.close()
        }
      }
      case Failure(e) => throw e
    }
  }
}