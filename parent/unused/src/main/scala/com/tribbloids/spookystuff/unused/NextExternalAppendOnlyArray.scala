//package com.tribbloids.spookystuff.unused
//
//import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
//import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
//import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
//import org.apache.spark.rdd.spookystuff.{ConsumedIterator, FallbackIterator, FastForwardingIterator}
//import org.apache.spark.serializer
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.util.collection.ExternalAppendOnlyMap
//import org.slf4j.LoggerFactory
//
//import java.util.concurrent.atomic.AtomicInteger
//import scala.collection.mutable.ArrayBuffer
//import scala.reflect.ClassTag
//
///**
//  * WARNING: scraped on job completion, if you want it to keep it across multiple tasks you need to launch a job with
//  * `spark.task.cpus = 0`
//  * @param ctag
//  *   used to automatically determine serializer being used
//  * @tparam T
//  *   affects ctg which is used in Ser/De
//  */
//class NextExternalAppendOnlyArray[T] private[spookystuff] (
//    batchSize: Long = 1024,
//    override val _lifespan: Lifespan = Lifespan.JVM.apply()
//) extends LocalCleanable {
//
//  val INCREMENT = 1024
//  val INCREMENT_LARGE = 65536
//
//  val delegate: ExternalAppendOnlyMap[Long, T, ArrayBuffer[T]] = new ExternalAppendOnlyMap(
//    { v => ArrayBuffer[T](v) },
//    { (c, v) =>
//      c.append(v)
//      c
//    },
//    { (v1, v2) =>
//      v1.appendAll(v2)
//      v2
//    }
//  )
//
//  delegate.insert
//
//  def addIfNew(i: Int, v: T): Unit = synchronized {
//
//    if (i == length) {
//      //      println(s"add $i")
//
//      list.add(v)
//    } else if (i > length) {
//
//      if (notLogged) {
//
//        notLogged = false
//
//        LoggerFactory
//          .getLogger(this.getClass)
//          .warn(s"new value at index $i is ahead of length $length and cannot be added")
//      }
//    }
//  }
//
//  def length: Int = {
//    list.size()
//  }
//
//  def sanity(): Unit = {
//
//    if (isCleaned) {
//      throw new IllegalStateException(s"Already scrapped for ID $id")
//    } else if (mapDB.isClosed) {
//
//      throw new IllegalStateException(s"External storage is closed for ID $id")
//    }
//  }
//
//  /**
//    * NOT thread safe
//    * @param index
//    *   iterator starts here
//    */
//  case class StartingFrom(index: Int = 0) {
//
//    {
//      sanity()
//    }
//
//    case object CachedIterator extends FastForwardingIterator[T] with ConsumedIterator with NOTSerializable {
//
//      protected val _offset = new AtomicInteger(index) // strictly incremental, index of the next pointer
//      override def offset: Int = _offset.get()
//
//      override protected def fastForward(n: Int): this.type = {
//
//        _offset.addAndGet(n)
//        this
//      }
//
//      override def hasNext: Boolean = {
//
//        offset < list.size()
//      }
//
//      override def next(): T = {
//
//        val result = list.get(offset)
//        _offset.incrementAndGet()
//        result
//      }
//    }
//
//    case class CachedOrComputeIterator(
//        doCompute: () => Iterator[T] with ConsumedIterator
//    ) extends FallbackIterator[T]
//        with NOTSerializable {
//
//      def outer: NextExternalAppendOnlyArray[T] = NextExternalAppendOnlyArray.this
//
//      override def getPrimary: Iterator[T] with ConsumedIterator = {
//
//        CachedIterator
//      }
//
//      case object ComputeAndAppendIterator
//          extends FastForwardingIterator[T]
//          with ConsumedIterator
//          with NOTSerializable {
//
//        lazy val computeIterator: Iterator[T] with ConsumedIterator = {
//
//          doCompute()
//        }
//
//        override def offset: Int = {
//
//          computeIterator.offset
//        }
//
//        override def hasNext: Boolean = {
//
//          computeIterator.hasNext
//        }
//
//        override def next(): T = NextExternalAppendOnlyArray.this.synchronized {
//
//          val currentOffset = computeIterator.offset
//          val result = computeIterator.next()
//
//          //          if (currentOffset > primary.offset)
//          addIfNew(currentOffset, result)
//
//          result
//        }
//
//        override protected def fastForward(n: Int): this.type = {
//
//          computeIterator.drop(n)
//          this
//        }
//      }
//
//      override def getBackup: Iterator[T] with ConsumedIterator = {
//
//        ComputeAndAppendIterator
//      }
//    }
//  }
//
//  def isEmpty: Boolean = {
//    list.isEmpty
//  }
//
//  /**
//    * can only be called once
//    */
//  override protected def cleanImpl(): Unit = {}
//}
//
//object NextExternalAppendOnlyArray {
//
//  val INCREMENT = 1024
//  val INCREMENT_LARGE = 65536
//
////  val existing: CachingUtils.ConcurrentCache[String, ExternalAppendOnlyArray[_]] = CachingUtils.ConcurrentCache()
//
//  def apply[T: ClassTag](
//      id: String,
//      storageLevel: StorageLevel,
//      serializerFactory: () => serializer.Serializer
//  ): NextExternalAppendOnlyArray[T] = {}
//
//  class CannotComputeException(info: String) extends ArrayIndexOutOfBoundsException(info)
//}
