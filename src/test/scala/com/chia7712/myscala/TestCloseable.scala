package com.chia7712.myscala

import java.io.IOException

import org.junit.{Assert, Test}
import org.scalatest.junit.AssertionsForJUnit
import com.chia7712.myscala.Closeable._
class TestCloseable extends AssertionsForJUnit {

  @Test
  def testDoCloseWithApply() {
    showName(new MyObject("name1"))
    showName(MyObject("name1"))
    Assert.assertEquals(2, MyObject.COUNT)
  }
  private[this] def showName(f: => MyObject) {
    println(f.name)
    println(f.name)
  }
  private[this] class MyObject(val name:String) {
    // empty
  }

  private[this] object MyObject {
    var COUNT = 0
    def apply(name:String) = {
      COUNT += 1
      new MyObject(name)
    }
  }

  @Test
  def testDoClose() {
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
