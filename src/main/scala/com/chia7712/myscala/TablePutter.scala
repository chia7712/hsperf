package com.chia7712.myscala

object TablePutter extends App{
  if (args.length < 2) {
    println("[OPTION] <table name> <fm0> ... <fmN>")
  } else {
    val conn = Connection()
    val tableName = args(0)
    val cfs = for ((cf, count) <- args.zipWithIndex if count != 0) yield {
      ByteUtil.toBytes(cf)
    }
    try {
      conn.recreateTable(tableName, cfs:_*)
      val table = conn.getTable(tableName)
      for (i <- 0 to 100) {
        val row = ByteUtil.toBytes(i)
        val value = ByteUtil.toBytes(i)
        val qualifier = ByteUtil.toBytes(i)
        table.put(cfs.map(cf => Cell(Key(row, cf, qualifier), value)):_*)
      }
    } finally {
      conn.close()
    }
  }
}

