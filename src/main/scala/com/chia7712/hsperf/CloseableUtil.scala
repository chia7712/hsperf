package com.chia7712.hsperf

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object CloseableUtil {
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

  def doClose[A <: java.io.Closeable, B](generator: => A) (worker: A => B) = {
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