package com.chia7712.hsperf

object TablePutter extends App{
  if (args.length < 2) {
    println("[OPTION] <table name> <row count> ")
  } else {
    CellStream(args(0), args(1).toInt)
      .withBatchSize(50)
      .withCellerThread(5)
      .withPutterThread(5)
      .run()
  }
}

