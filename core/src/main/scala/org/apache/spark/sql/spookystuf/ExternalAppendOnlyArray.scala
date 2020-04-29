package org.apache.spark.sql.spookystuf

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkEnv
import org.apache.spark.serializer.{Serializer, SerializerManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  * @param delegate only supports UnsafeRow, adapted
  * @param serializerMgr see below
  * @param ctg used to automatically determine serializer being used
  * @tparam T cannot be UnsafeRow itself, use the delegate directly in this case
  */
class ExternalAppendOnlyArray[T](
    val delegate: ExternalAppendOnlyUnsafeRowArray,
    val serializerMgr: SerializerManager
)(
    implicit val ctg: ClassTag[T]
) {

  import ExternalAppendOnlyArray._

  {
    require(
      ctg.getClass != classOf[UnsafeRow],
      "cannot store UnsafeRow, use ExternalAppendOnlyUnsafeRowArray directly"
    )
  }

  def this(numRowsInMemoryBufferThreshold: Int, numRowsSpillThreshold: Int)(
      implicit ctg: ClassTag[T]
  ) = {

    this(
      new ExternalAppendOnlyUnsafeRowArray(numRowsInMemoryBufferThreshold, numRowsSpillThreshold),
      SparkEnv.get.serializerManager
    )
  }

  val ser: Serializer = serializerMgr.getSerializer(ctg, autoPick = true)

  protected def disguise(v: T): UnsafeRow = {

    val bin = ser.newInstance().serialize(v).array()

    val result = new UnsafeRow()
    result.pointTo(bin, bin.length)
    result
  }

  protected def reveal(row: UnsafeRow): T = {

    val result = ser.newInstance().deserialize[T](ByteBuffer.wrap(row.getBytes))

    result
  }

  def add(v: T): Unit = {
    val row = disguise(v)
    delegate.add(row)
  }

  def addIfNew(i: Int, v: T): Unit = this.synchronized {

    if (i == length) add(v)
    else if (i > length)
      LoggerFactory
        .getLogger(this.getClass)
        .info(s"new value at index $i is out of order and cannot be added")

  }

  /**
    * NOT thread safe
    * @param startingFrom iterator starts here
    */
  case class Impl(startingFrom: Int = 0) {

    def snapshotIterator: Iterator[T] = {

      if (length <= startingFrom) Iterator.empty
      else {
        val raw = delegate.generateIterator(startingFrom)

        raw.map { row =>
          val result = reveal(row)
          result
        }
      }
    }

    case class cachedIterator() extends FastForwardingIterator[T] {

      val consumed = new AtomicInteger(startingFrom) // strictly incremental

      @transient var snapshot: Iterator[T] = _
      updateSnapshot()

      def updateSnapshot(): Unit = {
        snapshot = Impl(consumed.get()).snapshotIterator.map { v =>
          consumed.incrementAndGet()
//          println(s"cached pointer $v")
          v
        }
      }

      // once reaching the end, cannot become alive again
      @transient var _hasNext: Boolean = true
      override def hasNext: Boolean = {

        _hasNext = _hasNext && {
          consumed.get() < delegate.length
        }

        _hasNext
      }

      override def next(): T = {

        if (snapshot.hasNext) snapshot.next()
        else if (hasNext) {
          updateSnapshot()
          snapshot.next()
        } else {
          throw new NoSuchElementException("next on empty iterator")
        }
      }

      override def fastForward(n: Int): FastForwardingIterator[T] = Impl(consumed.get() + n).cachedIterator()
    }

    case class cachedOrComputeIterator(
        computeIterator: Iterator[T], // can be assumed to be thread exclusive
        computeStartingFrom: Int = 0
    ) extends FastForwardingIterator[T] {

      val cached: cachedIterator = cachedIterator()

      val computeConsumed = new AtomicInteger(computeStartingFrom)
      val overallConsumed = new AtomicInteger(startingFrom)

      lazy val computed: Iterator[T] = computeIterator.map { v =>
        computeConsumed.incrementAndGet()
        v
      }

      def computeCatchingUp(): Unit = {

        val difference = cached.consumed.get() - computeConsumed.get()

        if (difference < 0)
          throw new CannotComputeException(
            s"compute iterator can't go back: from $computeConsumed to ${cached.consumed}"
          )

        if (difference > 0)
          computed.drop(difference)
      }

      lazy val computeFastForwardOnce: Unit = computeCatchingUp()

      override def hasNext: Boolean = cached.hasNext || {
        computeFastForwardOnce
        computed.hasNext
      }

      override def next(): T = {

        val result = if (cached.hasNext) {
          cached.next()
        } else {
          computeFastForwardOnce
          val result = computed.next()

          addIfNew(computeConsumed.get() - 1, result)

          result
        }
        overallConsumed.incrementAndGet()
        result
      }

      override def fastForward(n: Int): FastForwardingIterator[T] =
        Impl(overallConsumed.get() + n).cachedOrComputeIterator(computeIterator, computeConsumed.get())
    }
  }

  def clear(): Unit = {
    delegate.clear()
  }

  def length: Int = {
    delegate.length
  }

  def isEmpty: Boolean = {
    delegate.isEmpty
  }
}

object ExternalAppendOnlyArray {

  class CannotComputeException(info: String) extends ArrayIndexOutOfBoundsException(info)
}
