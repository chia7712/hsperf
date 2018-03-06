package com.chia7712.myscala

import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.routing.{Broadcast, RoundRobinPool}
import com.chia7712.myscala.Closeable._
import org.apache.commons.logging.LogFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
class CellStream(private[this] val tableName:String, private[this] val rowCount:Int) {
  private[this] val LOG = LogFactory.getLog(CellStream.getClass)
  private[this] var putterThread = 5
  private[this] var cellerThread = 5
  private[this] var batchSize = 50
  def withPutterThread(n:Int): this.type = {
    putterThread = n
    this
  }
  def withCellerThread(n:Int): this.type = {
    cellerThread = n
    this
  }
  def withBatchSize(n:Int): this.type = {
    batchSize = n
    this
  }

  def run() = {
    doClose(Connection()) {
      conn => {
        val cfs = conn.getColumns(tableName)
        doFinally(ActorSystem("cellStream")) {
          actorSystem => {
            val putter = actorSystem.actorOf(RoundRobinPool(putterThread)
              .props(Props(new Putter(TableProxy(conn.getTable(tableName)), batchSize))), "putter")
            val celler = actorSystem.actorOf(RoundRobinPool(cellerThread)
              .props(Props(new Celler(putter, cfs))), "celler")
            var processedCount = 0
            val avg = rowCount / cellerThread
            while (processedCount < rowCount) {
              var toSend = if (rowCount > avg) avg else rowCount
              celler ! (processedCount, processedCount + toSend)
              processedCount += toSend
            }
            var cellCount = 0;
            val cellSum = rowCount * cfs.length
            do {
              cellCount = TableProxy.sum
              LOG.info(s"Total cells:$cellSum, processed:$cellCount")
              TimeUnit.SECONDS.sleep(5)
            } while (cellCount < cellSum)
            // send a stop flag to all actors
            celler ! Broadcast(PoisonPill)
            putter ! Broadcast(PoisonPill)
            actorSystem.stop(celler)
            actorSystem.stop(putter)
          }
        } {
          actorSystem:ActorSystem => Await.result(actorSystem.terminate(), 60 minute)
        }
      }
    }
  }


  class Celler(val outter:ActorRef, val cfs:Array[Array[Byte]]) extends Actor {
    override def receive = {
      case (start:Int, end:Int) => {
        for (i <- start until end) {
          val row = ByteUtil.toBytes(i)
          val value = ByteUtil.toBytes(i)
          val qualifier = ByteUtil.toBytes(i)
          cfs.foreach(cf => outter ! Cell(Key(row, cf, qualifier), value))
        }
      }
      case _ => LOG.info("Celler recevie garbage...")
    }
    override def postStop() = LOG.info("Celler end")
  }

  private[this] class Putter(val table:Table, bufferSize:Int) extends Actor {
    private[this] val buffer = new ArrayBuffer[Cell](bufferSize)
    override def receive = {
      case c:Cell => {
        if ((buffer += c).size >= bufferSize) {
          try {
            table.put(buffer)
          } finally {
            buffer.clear()
          }
        }
      }
    }
    override def postStop() = {
      table.close()
      LOG.info("Putter end")
    }
  }
  trait CellCounter {
    def cellCount:Int
  }

  private[this] class TableProxy(val table:Table) extends Table with CellCounter {
    var cellCount = 0
    override def put(cells: Seq[Cell]) = {
      try {
        table.put(cells)
      } finally {
        cellCount += cells.size
      }
    }

    override def delete(key:Seq[Key]) = {
      try {
        table.delete(key)
      } finally {
        cellCount += key.size
      }
    }

    override def deleteRow(row:Seq[Array[Byte]]) = {
      try {
        table.deleteRow(row)
      } finally {
        cellCount += row.size
      }
    }

    override def deleteFamily(rowFm:Seq[(Array[Byte], Array[Byte])]) = {
      try {
        table.deleteFamily(rowFm)
      } finally {
        cellCount += rowFm.size
      }
    }

    override def tableName = table.tableName

    override def close() = table.close()
  }

  private[this] object TableProxy {
    val COUNTERS = new ConcurrentLinkedQueue[CellCounter]
    def sum = COUNTERS.stream().mapToInt(_.cellCount).sum
    def apply(table:Table)  = {
      val rval = new TableProxy(table)
      COUNTERS.add(rval)
      rval
    }
  }
}





object CellStream {
  def apply(tableName:String, cellCount:Int) = new CellStream(tableName, cellCount)
}

