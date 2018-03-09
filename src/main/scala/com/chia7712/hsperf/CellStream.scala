package com.chia7712.hsperf

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.routing.Broadcast
import akka.routing.RoundRobinPool
import com.chia7712.hsperf.CloseableUtil._
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.logging.LogFactory
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern.gracefulStop
class CellStream(private[this] val tableName:String, private[this] val rowCount:Int) {
  private[this] val LOG = LogFactory.getLog(CellStream.getClass)
  private[this] var putterThread = 5
  private[this] var cellerThread = 5
  private[this] var batchSize = 50
  def withPutterThread(n:Int): this.type = {
    PartialFunction
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

  def createPutter(actorSystem:ActorSystem, table: => Table) = {
    actorSystem.actorOf(RoundRobinPool(putterThread)
      .props(Props(new Putter(table, batchSize))), "putter")
  }
  private[this] def createCeller(actorSystem:ActorSystem, putter:ActorRef, cfs:Array[Array[Byte]]) = {
    actorSystem.actorOf(RoundRobinPool(cellerThread)
      .props(Props(new Celler(putter, cfs))), "celler")
  }

  private[this] def dispatch(celler:ActorRef) = {
    var processedCount = 0
    val avg = rowCount / cellerThread
    while (processedCount < rowCount) {
      var toSend = if (rowCount > avg) avg else rowCount
      celler ! (processedCount, processedCount + toSend)
      processedCount += toSend
    }
  }

  private[this] def logUntilDone(cellSum:Long) = {
    var cellCount = 0;
    do {
      TimeUnit.SECONDS.sleep(5)
      cellCount = CellCounter.sum
      LOG.info(s"Total cells:$cellSum, processed:$cellCount")
    } while (cellCount < cellSum)
  }

  def run() = {
    doClose(Connection()) {
      conn => {
        val cfs = conn.getColumns(tableName)
        doFinally(ActorSystem("cellStream")) {
          actorSystem => {
            val putter = createPutter(actorSystem, conn.getTable(tableName, CellCounter()))
            val celler = createCeller(actorSystem, putter, cfs)
            dispatch(celler)
            logUntilDone(cfs.length * rowCount)
            // send a stop flag to all actors
            Await.result(gracefulStop(celler, 60 minute, Broadcast(PoisonPill)), 60 minute)
            Await.result(gracefulStop(putter, 60 minute, Broadcast(PoisonPill)), 60 minute)
          }
        } {
          actorSystem:ActorSystem => Await.result(actorSystem.terminate(), 60 minute)
        }
      }
    }
  }


  private[this] class Celler(val outter:ActorRef, val cfs:Array[Array[Byte]]) extends Actor {
    override def receive = {
      case (start:Int, end:Int) => {
        for (i <- start until end) {
          val row = ByteUtil.toBytes(i.toString)
          val value = ByteUtil.toBytes(i.toString)
          val qualifier = ByteUtil.toBytes(i.toString)
          cfs.foreach(cf => outter ! KeyValue(Key(row, cf, qualifier), value))
        }
      }
      case _ => LOG.info("Celler recevie garbage...")
    }
    override def postStop() = LOG.info("Celler end")
  }

  private[this] class Putter(val table:Table, bufferSize:Int) extends Actor {
    private[this] val buffer = new ArrayBuffer[KeyValue](bufferSize)
    override def receive = {
      case c:KeyValue => {
        if ((buffer += c).size >= bufferSize) {
          try {
            table.putCells(buffer)
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

  private[this] class CellCounter private extends TableObserver {
    val count = new AtomicInteger(0)
    override def postPutCells(cells:Seq[KeyValue]):Unit = {
      count.addAndGet(cells.size)
    }
  }
  private[this] object CellCounter {
    val COUNTERS = new ConcurrentLinkedQueue[CellCounter]
    def sum = COUNTERS.stream().mapToInt(_.count.get).sum
    def apply() = {
      val counter = new CellCounter
      COUNTERS.add(counter)
      counter
    }
  }
}

object CellStream {
  def apply(tableName:String, cellCount:Int) = new CellStream(tableName, cellCount)
}

