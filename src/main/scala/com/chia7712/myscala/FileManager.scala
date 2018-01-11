package main.scala.com.chia7712.myscala

import java.io.File

object FIleManager {
  def main(args:Array[String]) {
    val folder = new File("/home/chia7712");

    val names = for (file <- folder.listFiles().toList
      if file.isFile
      if !file.isHidden) yield {
      file.getName
    }

    println(names)

  }
}
