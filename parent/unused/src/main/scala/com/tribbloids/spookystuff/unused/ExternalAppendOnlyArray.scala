package com.tribbloids.spookystuff.unused

import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import com.tribbloids.spookystuff.utils.{CachingUtils, CommonConst, CommonUtils, ThreadLocal}
import org.apache.spark.rdd.spookystuff.{ConsumedIterator, FallbackIterator, FastForwardingIterator}
import org.apache.spark.serializer
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.StorageLevel
import org.mapdb._
import org.mapdb.serializer.GroupSerializerObjectArray
import org.slf4j.LoggerFactory

import java.io._
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

/**
  * WARNING: scraped on job completion, if you want it to keep it across multiple tasks you need to launch a job with
  * `spark.task.cpus = 0`
  * @param ctag
  *   used to automatically determine serializer being used
  * @tparam T
  *   affects ctg which is used in Ser/De
  */
class ExternalAppendOnlyArray[T] private[spookystuff] (
    id: String,
    storageLevel: StorageLevel,
    serializerFactory: () => serializer.Serializer,
    override val _lifespan: Lifespan = Lifespan.JVM.apply()
)(
    implicit
    val ctag: ClassTag[T]
) extends LocalCleanable {

  val INCREMENT = 1024
  val INCREMENT_LARGE = 65536

  import ExternalAppendOnlyArray._

  val filePath: String = CommonUtils.\\\(
    rootPath,
    processID,
    id
  )

  {
    if (existing.contains(id)) {

      throw new IllegalStateException("same ID already existed")
    }
    LoggerFactory.getLogger(this.getClass).debug(s"created for ID $id -> $filePath")
  }

  @transient lazy val file: File = {

    val result = new File(filePath)

    if (!result.exists()) result.getParentFile.mkdirs()

    result.deleteOnExit()

    result
  }

  @transient lazy val mapDB: DB = {

    def fileDBMaker = {

      lazy val basic = DBMaker
        .fileDB(file)
//        .fileChannelEnable()
        .fileDeleteAfterClose()
      //        .transactionEnable()

      lazy val mmap = basic
        .fileMmapEnableIfSupported()
//        .fileMmapPreclearDisable()
//        .cleanerHackEnable()

      mmap
    }

    val result = storageLevel match {
      case StorageLevel.MEMORY_AND_DISK_SER =>
        fileDBMaker
          .allocateStartSize(INCREMENT_LARGE)
          .allocateIncrement(INCREMENT_LARGE)
          .make()

      case StorageLevel.DISK_ONLY =>
        fileDBMaker
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

    result.checkThreadSafe()

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

  // tree list is not the fastest for this task
  @transient lazy val list: IndexTreeList[T] = {

    //    println(s"new backbone in ${TaskContext.get().taskAttemptId()}!")

    val treeList = mapDB
      .indexTreeList(id, SerDe)
      .createOrOpen()

    require(treeList.isThreadSafe)

    treeList
  }

  def length: Int = {
    list.size()
  }

  @volatile var notLogged = true
  def addIfNew(i: Int, v: T): Unit = synchronized {

    if (i == length) {
      //      println(s"add $i")

      list.add(v)
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

    if (isCleaned) {
      throw new IllegalStateException(s"Already scrapped for ID $id")
    } else if (mapDB.isClosed) {

      throw new IllegalStateException(s"External storage is closed for ID $id")
    }
  }

  /**
    * NOT thread safe
    * @param index
    *   iterator starts here
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

        offset < list.size()
      }

      override def next(): T = {

        val result = list.get(offset)
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
    list.isEmpty
  }

  /**
    * can only be called once
    */
  override protected def cleanImpl(): Unit = ExternalAppendOnlyArray.existing.synchronized {

    val id = this.id

    list.clear()
    mapDB.close()
    file.delete()

    LoggerFactory.getLogger(this.getClass).debug(s"scrapped for ID $id -> ${file.getPath}")

    ExternalAppendOnlyArray.existing -= id
  }
}

object ExternalAppendOnlyArray {

  val INCREMENT = 1024
  val INCREMENT_LARGE = 65536

  val rootPath: String = CommonUtils.\\\(
    CommonConst.ROOT_TEMP_DIR,
    classOf[ExternalAppendOnlyArray[_]].getSimpleName
  )

  val processID: String = UUID.randomUUID().toString

  val existing: CachingUtils.ConcurrentCache[String, ExternalAppendOnlyArray[_]] = CachingUtils.ConcurrentCache()

  def apply[T: ClassTag](
      id: String,
      storageLevel: StorageLevel,
      serializerFactory: () => serializer.Serializer
  ): ExternalAppendOnlyArray[T] = existing.synchronized {

    val result = new ExternalAppendOnlyArray[T](id, storageLevel, serializerFactory)

    existing.put(id, result)

    result
  }

  case class LocalRef[T: ClassTag](
      id: String,
      storageLevel: StorageLevel,
      serializerFactory: () => serializer.Serializer
  ) extends Serializable {

    @transient lazy val get: ExternalAppendOnlyArray[T] = {

      existing
        .getOrElseUpdate(
          id,
          ExternalAppendOnlyArray(id, storageLevel, serializerFactory)
        )
        .asInstanceOf[ExternalAppendOnlyArray[T]]
    }
  }

  class CannotComputeException(info: String) extends ArrayIndexOutOfBoundsException(info)

}
