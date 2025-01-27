package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.*
import com.tribbloids.spookystuff.commons.CommonUtils
import com.tribbloids.spookystuff.io.{Progress, WriteMode}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkEnv
import org.slf4j.LoggerFactory

import java.util.Date

object DocUtils {

  def dfsRead[T](logMsg: String, errorMsg: String, spooky: SpookyContext)(f: Progress => T): T = {
    try {
      val result = CommonUtils.retry(Const.DFSLocalRetries) {
        val progress = Progress()
        CommonUtils.withTimeout(spooky.conf.DFSTimeout)(
          f(progress),
          progress.defaultHeartbeat
        )
      }
      spooky.spookyMetrics.DFSReadSuccess += 1
      result
    } catch {
      case e: Exception =>
        spooky.spookyMetrics.DFSReadFailure += 1
        val ex = new DFSReadException(errorMsg, e)

        if (spooky.conf.failOnDFSRead) throw ex
        else {
          LoggerFactory.getLogger(this.getClass).warn(logMsg, ex)
          null.asInstanceOf[T]
        }
    }
  }

  // always fail on retry depletion and timeout
  def dfsWrite[T](logMsg: String, errorMsg: String, spooky: SpookyContext)(f: Progress => T): T = {
    try {
      val result = CommonUtils.retry(Const.DFSLocalRetries) {
        val progress = Progress()
        CommonUtils.withTimeout(spooky.conf.DFSTimeout)(
          f(progress),
          progress.defaultHeartbeat
        )
      }
      spooky.spookyMetrics.DFSWriteSuccess += 1
      result
    } catch {
      case e: Exception =>
        spooky.spookyMetrics.DFSWriteFailure += 1
        val ex = new DFSWriteException(errorMsg, e)
        LoggerFactory.getLogger(this.getClass).error(logMsg, ex)
        throw ex
//        if (spooky.spookyConf.failOnDFSRead) throw ex // TODO: add failOnDFSWrite
//        else {
//          LoggerFactory.getLogger(this.getClass).warn(message, ex)
//          null.asInstanceOf[T]
//        }
    }
  }

  def load(pathStr: String)(spooky: SpookyContext): Array[Byte] =
    dfsRead("load", pathStr, spooky) { progress =>
      val result = spooky.pathResolver.input(pathStr) { in =>
        IOUtils.toByteArray(progress.WrapIStream(in.stream))
      }

      result
    }

  final val cacheVID = 91252374923L

  // unlike save, this will store all information in an unreadable, serialized, probably compressed file
  // always overwrite! use the same serializer as Spark
  def cache[T](
      pageLikes: Seq[T],
      pathStr: String,
      overwrite: Boolean = true
  )(spooky: SpookyContext): Unit =
    dfsWrite("cache", pathStr, spooky) { progress =>
      val list = pageLikes.toList // Seq may be a stream that cannot be serialized

      val mode =
        if (overwrite) WriteMode.Overwrite
        else WriteMode.CreateOnly

      spooky.pathResolver.output(pathStr, mode) { out =>
        val ser = SparkEnv.get.serializer.newInstance()
        val serOut = ser.serializeStream(progress.WrapOStream(out.stream))

        try {
          serOut.writeObject(
            list.asInstanceOf[List[T] @SerialVersionUID(cacheVID) & Serializable]
          )
        } finally {
          serOut.close()
        }
      }
    }

  private def restore[T](pathStr: String)(spooky: SpookyContext): Seq[T] =
    dfsRead("restore", pathStr, spooky) { progress =>
      val result = spooky.pathResolver.input(pathStr) { in =>
        val ser = SparkEnv.get.serializer.newInstance()

        val serIn = ser.deserializeStream(progress.WrapIStream(in.stream))
        try {
          serIn.readObject[List[T]]()
        } finally {
          serIn.close()
        }
      }

      result
    }

  // restore latest in a directory
  // returns: Nil => has backtrace dir but contains no page
  // returns null => no backtrace dir
  def restoreLatest(
      dirPath: Path,
      earliestModificationTime: Long,
      latestModificationTime: Long
  )(spooky: SpookyContext): Seq[Observation] = {

    val latestStatus: Option[FileStatus] =
      dfsRead("get latest version", dirPath.toString, spooky) { _ =>
        val fs = dirPath.getFileSystem(spooky.hadoopConf)

        if (fs.exists(dirPath) && fs.getFileStatus(dirPath).isDirectory) {

          val statuses = fs.listStatus(dirPath)

          statuses
            //          .filter(status => !status.isDirectory && status.getModificationTime >= earliestModificationTime - 300*1000) //Long enough for overhead of eventual consistency to take effect and write down file
            .filter(_.getModificationTime < latestModificationTime) // TODO: may have disk write delay!
            .sortBy(_.getModificationTime)
            .lastOption
        } else None
      }

    latestStatus match {
      case Some(status) =>
        val results = restore[Observation](status.getPath.toString)(spooky)

        if (results == null) {
          LoggerFactory.getLogger(this.getClass).warn("Cached content is corrputed:\n" + dirPath)
          null
        } else if (results.head.timeMillis >= earliestModificationTime) results
        else {
          LoggerFactory
            .getLogger(this.getClass)
            .info(
              s"All cached contents has become obsolete after " +
                s"${new Date(earliestModificationTime).toString}: " +
                dirPath
            )
          null
        }
      case _ =>
        LoggerFactory.getLogger(this.getClass).info(s"Not cached: " + dirPath)
        null
    }
  }
}
