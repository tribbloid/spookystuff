package org.apache.spark.sql.spookystuf

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkEnv
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray

import scala.reflect.ClassTag

/**
  * NOT thread safe!
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

  lazy val ser: SerializerInstance = serializerMgr.getSerializer(ctg, autoPick = true).newInstance()

  def disguise(v: T): UnsafeRow = {

    val bin = ser.serialize(v).array()

    val result = new UnsafeRow()
    result.pointTo(bin, bin.length)
    result
  }

  def reveal(row: UnsafeRow): T = {

    val result = ser.deserialize[T](ByteBuffer.wrap(row.getBytes))

    result
  }

  def add(v: T): Unit = {
    val row = disguise(v)
    delegate.add(row)
  }

  def generateIterator(outputStart: Int = 0): Iterator[T] = {

    val raw = delegate.generateIterator(outputStart)

    raw.map { row =>
      reveal(row)
    }
  }

  /**
    * @param fallback assumed to be slow, will be avoided whenever possible
    * @param outputStart of the first returned value
    * @return
    */
  def getOrComputeIterator(
      fallback: Iterator[T],
      fallbackTraversed: Int = 0,
      outputStart: Int = 0
  ): Iterator[T] = {

    val offsetCounter = new AtomicInteger(outputStart - fallbackTraversed)

    val cached = generateIterator(outputStart).map { v =>
      offsetCounter.getAndIncrement()
      v
    }

    lazy val offsetAfterCacheIsDrained = {

      val _offset = offsetCounter.get()

      if (_offset < 0)
        throw new ArrayIndexOutOfBoundsException(
          s"fallback iterator can no longer provide values from ${fallbackTraversed + _offset} ~ $fallbackTraversed"
        )

      _offset
    }

    val computed = {

      fallback.slice(offsetCounter.get(), Int.MaxValue).map { v =>
        offsetAfterCacheIsDrained
        add(v)
        v
      }
    }

    cached ++ computed
  }

  //TODO: remove, redundant
  //    case class GetOrCompute(
  //                             fallback: Iterator[T],
  //                             index: Int = 0
  //                    ) extends Iterator[T] {
  //
  //      lazy val cached: Iterator[T] = array.generateIterator(index)
  //
  //
  //
  //      override def hasNext: Boolean = {
  //
  //        cached.hasNext || fallback.hasNext
  //      }
  //
  //      val offset: AtomicInteger = new AtomicInteger(index)
  //
  //      override def next(): T = {
  //
  //        if (cached.hasNext) {
  //          offset.getAndIncrement()
  //          cached.next()
  //        }
  //        else {
  //
  //        }
  //      }
  //    }

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
