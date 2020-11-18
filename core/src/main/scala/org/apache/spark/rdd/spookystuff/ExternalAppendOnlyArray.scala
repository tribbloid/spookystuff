package org.apache.spark.rdd.spookystuff

import java.io._
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.utils.ThreadLocal
import com.tribbloids.spookystuff.utils.lifespan.{Lifespan, LocalCleanable}
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import org.apache.spark.serializer
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.StorageLevel
import org.mapdb._
import org.mapdb.serializer.GroupSerializerObjectArray
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  * WARNING: scraped on job completion, if you want it to keep it across multiple tasks you need to
  * launch a job with `spark.task.cpus = 0`
  * @param ctag used to automatically determine serializer being used
  * @tparam T affects ctg which is used in Ser/De
  */
class ExternalAppendOnlyArray[T](
    val name: String,
    val storageLevel: StorageLevel,
    val serializerFactory: () => serializer.Serializer,
    override val _lifespan: Lifespan = Lifespan.JVM()
)(
    implicit val ctag: ClassTag[T]
) extends Serializable
    with LocalCleanable {

  val INCREMENT = 1024
  val INCREMENT_LARGE = 65536

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

  @transient lazy val mapDB: DB = {
    val result = storageLevel match {
      case StorageLevel.MEMORY_AND_DISK_SER =>
        DBMaker
          .fileDB(dbTempFile)
          .fileMmapEnableIfSupported()
          .fileDeleteAfterClose()
          //
          .allocateStartSize(INCREMENT_LARGE)
          .allocateIncrement(INCREMENT_LARGE)
          .make()

      case StorageLevel.DISK_ONLY =>
        DBMaker
          .fileDB(dbTempFile)
          .fileMmapEnableIfSupported()
          .fileDeleteAfterClose()
          //
          .allocateStartSize(INCREMENT)
          .allocateIncrement(INCREMENT)
          .make()

      case StorageLevel.MEMORY_ONLY =>
        DBMaker
          .heapDB()
          .allocateStartSize(INCREMENT)
          .allocateIncrement(INCREMENT)
          .make()

      case StorageLevel.MEMORY_ONLY_SER =>
        DBMaker
          .memoryDB()
          .allocateStartSize(INCREMENT_LARGE)
          .allocateIncrement(INCREMENT_LARGE)
          .make()

      case StorageLevel.OFF_HEAP =>
        DBMaker
          .memoryDirectDB()
          .allocateStartSize(INCREMENT_LARGE)
          .allocateIncrement(INCREMENT_LARGE)
          .make()

      case _ =>
        throw new UnsupportedOperationException("Unsupported StorageLevel")
    }

    result
    // TODO: add more StorageLevels
  }

  @transient case object SerDe extends GroupSerializerObjectArray[T] {

    object SparkSerDe {

      val serDe: serializer.Serializer = serializerFactory()

      val factory: ThreadLocal[SerializerInstance] = ThreadLocal { _ =>
        serDe.newInstance()
      }

      def instance: SerializerInstance = {

        //      ser.newInstance()
        factory.get()
      }
    }

    override def serialize(out: DataOutput2, value: T): Unit = {

      val stream = SparkSerDe.instance.serializeStream(out)

      stream.writeValue(value)

      stream.flush()
    }

    override def deserialize(input: DataInput2, available: Int): T = {

      val stream = SparkSerDe.instance.deserializeStream(
        DataInput2AsStream(input)
      )

      val result = stream.readValue[T]()

//      require(result != null, "deserialization failed, value cannot be null")

      result
    }
  }

  @transient lazy val backbone: IndexTreeList[T] = {

//    println(s"new backbone in ${TaskContext.get().taskAttemptId()}!")

    val treeList = mapDB
      .indexTreeList(id, SerDe)
      .createOrOpen()

    require(treeList.isThreadSafe)

    treeList
  }

  def length: Int = {
    backbone.size()
  }

  @volatile var notLogged = true
  def addIfNew(i: Int, v: T): Unit = synchronized {

    if (i == length) {
//      println(s"add $i")

      backbone.add(v)
    } else if (i > length) {

      if (notLogged) {

        notLogged = false

        LoggerFactory
          .getLogger(this.getClass)
          .warn(s"new value at index $i is ahead of length $length and cannot be added")
      }
    }
  }

  def sanity(): Unit = {

    if (isCleaned ||
        mapDB.isClosed) {

      throw new UnsupportedOperationException("External storage is closed")
    }
  }

  /**
    * NOT thread safe
    * @param index iterator starts here
    */
  case class StartingFrom(index: Int = 0) {

    {
      sanity()
    }

    case object CachedIterator extends FastForwardingIterator[T] with ConsumedIterator with NOTSerializable {

      protected val _offset = new AtomicInteger(index) // strictly incremental, index of the next pointer
      override def offset: Int = _offset.get()

      override protected def fastForward(n: Int): this.type = {

        _offset.addAndGet(n)
        this
      }

      override def hasNext: Boolean = {

        offset < backbone.size()
      }

      override def next(): T = {

        val result = backbone.get(offset)
        _offset.incrementAndGet()
        result
      }
    }

    case class CachedOrComputeIterator(
        doCompute: () => Iterator[T] with ConsumedIterator
    ) extends FallbackIterator[T]
        with NOTSerializable {

      def outer: ExternalAppendOnlyArray[T] = ExternalAppendOnlyArray.this

      override def getPrimary: Iterator[T] with ConsumedIterator = {

        CachedIterator
      }

      case object ComputeAndAppendIterator
          extends FastForwardingIterator[T]
          with ConsumedIterator
          with NOTSerializable {

        lazy val computeIterator: Iterator[T] with ConsumedIterator = {

          doCompute()
        }

        override def offset: Int = {

          computeIterator.offset
        }

        override def hasNext: Boolean = {

          computeIterator.hasNext
        }

        override def next(): T = ExternalAppendOnlyArray.this.synchronized {

          val currentOffset = computeIterator.offset
          val result = computeIterator.next()

          //          if (currentOffset > primary.offset)
          addIfNew(currentOffset, result)

          result
        }

        override protected def fastForward(n: Int): this.type = {

          computeIterator.drop(n)
          this
        }
      }

      override def getBackup: Iterator[T] with ConsumedIterator = {

        ComputeAndAppendIterator
      }
    }
  }

  def isEmpty: Boolean = {
    backbone.isEmpty
  }

  /**
    * can only be called once
    */
  override protected def cleanImpl(): Unit = {

    backbone.clear()
    mapDB.close()
  }
}

object ExternalAppendOnlyArray {

  class CannotComputeException(info: String) extends ArrayIndexOutOfBoundsException(info)

//  val existing: ConcurrentCache[String, ExternalAppendOnlyArray[_]] = ConcurrentCache()

  /**
    * Wraps [[DataInput]] into [[InputStream]]
    * see https://github.com/jankotek/mapdb/issues/971
    */
  case class DataInput2AsStream(in: DataInput2) extends InputStream {

    override def read(b: Array[Byte], off: Int, len: Int): Int = {

      val srcArray = in.internalByteArray()
      val srcBuffer = in.internalByteBuffer()

      val _len =
        if (srcArray != null) Math.min(srcArray.length, len)
        else if (srcBuffer != null) Math.min(srcBuffer.remaining(), len)
        else len

      val pos = in.getPos
//      val pos2Opt = Option(in.internalByteBuffer()).map(_.position())

      try {
        in.readFully(b, off, _len)
        _len
      } catch {

        // inefficient way
        case e: RuntimeException =>
//          Option(in.internalByteBuffer()).foreach { v =>
//            v.rewind()
//            v.position(pos2Opt.get)
//          } // no need, always 0
          in.setPos(pos)

          (off until (off + len)).foreach { i =>
            try {

              val next = in.readByte()
              b.update(i, next)
            } catch {
              case _: EOFException | _: RuntimeException =>
                return i
            }
          }
          len
      }
    }

    override def skip(n: Long): Long = {
      val _n = Math.min(n, Integer.MAX_VALUE)
      //$DELAY$
      in.skipBytes(_n.toInt)
    }

    override def close(): Unit = {
      in match {
        case closeable: Closeable => closeable.close()
        case _                    =>
      }
    }

    override def read: Int = in.readUnsignedByte
  }

}
