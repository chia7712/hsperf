package com.chia7712.myscala

import java.io.IOException

import org.junit.{Assert, Test}
import org.scalatest.junit.AssertionsForJUnit
import com.chia7712.myscala.Closeable._
class TestCloseable extends AssertionsForJUnit {


  @Test
  def testDoClose() = {
    def invalidString():Closeable = throw new IOException("IOE")
    try {
      doClose(invalidString)(_ => true)
      fail("It should fail")
    } catch {
      case _:IOException => {}
    }

    def validString():Closeable = () => {}
    Assert.assertTrue(doClose(validString) (_ => true))
    try {
      doFinally(invalidString)(_ => true) (_ => {})
      fail("It should fail")
    } catch {
      case _:IOException => {}
    }
    Assert.assertTrue(doFinally(validString)(_ => true)(_ => {}))
  }
}
