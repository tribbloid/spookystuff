package org.apache.spark.sql.spookystuf

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.utils.ThreadLocal
import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.{serializer, SparkEnv}
import org.mapdb._
import org.mapdb.serializer.GroupSerializerObjectArray
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  * WARNING: scraped on job completion, if you want it to keep it across multiple tasks you need to
  * launch a job with `spark.task.cpus = 0`
  * @param ctg used to automatically determine serializer being used
  * @tparam T affects ctg which is used to determine which serializer to use
  */
class ExternalAppendOnlyArray[T](
    val name: String,
    override val _lifespan: Lifespan = Lifespan.JVM()
)(
    implicit val ctg: ClassTag[T]
) extends Serializable
    with LocalCleanable {

  import ExternalAppendOnlyArray._

  val id = s"$name-${UUID.randomUUID()}"

  val dbTempFile: File = {

    val file = File.createTempFile("mapdb", s"-$id")
    file.delete()
    file.deleteOnExit()
    file
  }

//  {
//    if (existing.contains(id)) sys.error("same ID already existed")
//    existing += id -> this
//  }

  @transient lazy val db: DB = {
    val result = DBMaker
      .fileDB(dbTempFile)
      .fileMmapEnableIfSupported()
      .fileDeleteAfterClose()
      .make()

    result
  }

  @transient case object SerDe extends GroupSerializerObjectArray[T] {

    val serializerMgr: SerializerManager = SparkEnv.get.serializerManager

    object SparkSerDe {

      val ser: serializer.Serializer = serializerMgr.getSerializer(ctg, autoPick = true)

      val factory: ThreadLocal[SerializerInstance] = ThreadLocal { _ =>
        ser.newInstance()
      }

      def instance: SerializerInstance = {

        //      ser.newInstance()
        factory.get()
      }
    }

    override def serialize(out: DataOutput2, value: T): Unit = {

      val stream = SparkSerDe.instance.serializeStream(out)

      stream.writeObject(value)

      stream.flush()
    }

    override def deserialize(input: DataInput2, available: Int): T = {

      if (available == 0) {

        throw new IOException("zero byte")

      } else {

        val result = SparkSerDe.instance.deserialize[T](ByteBuffer.wrap(input.internalByteArray()))

        result
      }
    }
  }

  @transient lazy val backbone: IndexTreeList[T] = {

    val treeList = db.indexTreeList(id, SerDe).createOrOpen()

    require(treeList.isThreadSafe)

    treeList
  }

  {
    require(
      !classOf[InternalRow].isAssignableFrom(ctg.getClass),
      "cannot store InternalRow, use UnsafeRow to enable faster serializer"
    )
  }

  def add(v: T): Unit = {
    backbone.add(v)
  }

  def set(i: Int, v: T): Unit = {
    backbone.set(i, v)
  }

  def length: Int = {
    backbone.size()
  }

  @volatile var notLogged = true
  def addIfNew(i: Int, v: T): Unit = synchronized {

    if (i == length) {
      add(v)
    } else if (i > length && notLogged) {

      notLogged = false

      LoggerFactory
        .getLogger(this.getClass)
        .info(s"new value at index $i is ahead of length $length and cannot be added")
    }

  }

  /**
    * NOT thread safe
    * @param index iterator starts here
    */
  case class StartingFrom(index: Int = 0) {

    case class CachedIterator() extends FastForwardingIterator[T] with NOTSerializable {

      val consumed = new AtomicInteger(index) // strictly incremental, index of the next pointer

      override def fastForward(n: Int): FastForwardingIterator[T] = StartingFrom(consumed.get() + n).CachedIterator()

      override def hasNext: Boolean = {
        consumed.get() < backbone.size()
      }

      override def next(): T = {

        val result = backbone.get(consumed.get())
        skip()
        result
      }

      def skip(): Int = {

        consumed.incrementAndGet()
      }
    }

    // TODO: this should be another class
    case class CachedOrComputeIterator(
        computeIterator: Iterator[T], // can be assumed to be thread exclusive
        computeStartingFrom: Int = 0
    ) extends FastForwardingIterator[T]
        with NOTSerializable {

      val cached: CachedIterator = CachedIterator()

      val computeConsumed = new AtomicInteger(computeStartingFrom)
      val overallConsumed = new AtomicInteger(index)

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

      lazy val computeCachingUpOnce: Unit = {
        computeCatchingUp()
      }

      override def hasNext: Boolean = {
        cached.hasNext || {
          computeCachingUpOnce
          computed.hasNext
        }
      }

      override def next(): T = ExternalAppendOnlyArray.this.synchronized {

        val result = if (cached.hasNext) {
          cached.next()
        } else {
          computeCachingUpOnce
          val result = computed.next()
//          println("compute:" + result)

          addIfNew(computeConsumed.get() - 1, result)
          cached.skip()
          result
        }
        overallConsumed.incrementAndGet()

        result
      }

      override def fastForward(n: Int): FastForwardingIterator[T] =
        StartingFrom(overallConsumed.get() + n).CachedOrComputeIterator(computeIterator, computeConsumed.get())
    }
  }

  def clear(): Unit = {
    backbone.clear()
  }

  def isEmpty: Boolean = {
    backbone.isEmpty
  }

  /**
    * can only be called once
    */
  override protected def cleanImpl(): Unit = {

    backbone.clear()
    db.close()
  }
}

object ExternalAppendOnlyArray {

  class CannotComputeException(info: String) extends ArrayIndexOutOfBoundsException(info)

//  val existing: ConcurrentCache[String, ExternalAppendOnlyArray[_]] = ConcurrentCache()

}
