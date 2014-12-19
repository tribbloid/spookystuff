package org.tribbloid.spookystuff.pages

import java.util.UUID

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.actions.Trace
import org.tribbloid.spookystuff.utils.Utils
import org.tribbloid.spookystuff.views.Serializable
import org.tribbloid.spookystuff.{Const, DFSReadException, DFSWriteException, SpookyContext}

/**
 * Created by peng on 11/27/14.
 */
object PageUtils {

  def DFSRead[T](message: String, pathStr: String, spooky: SpookyContext)(f: => T): T = {
    try {
      val result = Utils.retry(Const.DFSLocalRetry) {
        Utils.withDeadline(spooky.DFSTimeout) {f}
      }
      spooky.metrics.DFSReadSuccess += 1
      result
    }
    catch {
      case e: Throwable =>
        spooky.metrics.DFSReadFail += 1
        val ex = new DFSReadException(pathStr ,e)
        ex.setStackTrace(e.getStackTrace)
        if (spooky.failOnDFSError) throw ex
        else {
          LoggerFactory.getLogger(this.getClass).warn(message, ex)
          null.asInstanceOf[T]
        }
    }
  }

  //always fail on retry depletion and timeout
  def DFSWrite[T](message: String, pathStr: String, spooky: SpookyContext)(f: => T): T = {
    try {
      val result = Utils.retry(Const.DFSLocalRetry) {
        Utils.withDeadline(spooky.DFSTimeout) {f}
      }
      spooky.metrics.DFSWriteSuccess += 1
      result
    }
    catch {
      case e: Throwable =>
        spooky.metrics.DFSWriteFail += 1
        val ex = new DFSWriteException(pathStr ,e)
        ex.setStackTrace(e.getStackTrace)
        throw ex
    }
  }

  def later(v1: PageLike, v2: PageLike): PageLike = {
    if (v1.timestamp after v2.timestamp) v1
    else v2
  }

  //TODO: return option
  def load(fullPath: Path)(spooky: SpookyContext): Array[Byte] = {

    DFSRead("load", fullPath.toString, spooky) {
      val fs = fullPath.getFileSystem(spooky.hConf)

      if (fs.exists(fullPath)) {

        val fis = fs.open(fullPath)

        try {
          IOUtils.toByteArray(fis) //TODO: according to past experience, IOUtils is not stable?
        }
        finally {
          fis.close()
        }
      }
      else null
    }
  }

  //unlike save, this will store all information in an unreadable, serialized, probably compressed file
  //always overwrite
  def cache(
             pageLikes: Seq[PageLike],
             path: String,
             overwrite: Boolean = false
             )(spooky: SpookyContext): Unit = {

    DFSWrite("cache", path, spooky) {
      val fullPath = new Path(path)

      val fs = fullPath.getFileSystem(spooky.hConf)

      val ser = SparkEnv.get.serializer.newInstance()
      val fos = fs.create(fullPath, overwrite)
      val serOut = ser.serializeStream(fos)

      try {
        serOut.writeObject[Seq[PageLike]](Serializable[Seq[PageLike]](pageLikes, 91252374923L))
      }
      finally {
        fos.close()
        serOut.close()
      }
    }
  }

  def autoCache(
                 pageLikes: Seq[PageLike],
                 spooky: SpookyContext
                 ): Unit = {
    val pathStr = Utils.urlConcat(
      spooky.autoCacheRoot,
      spooky.cacheTraceEncoder(pageLikes.head.uid.backtrace).toString,
      UUID.randomUUID().toString
    )

    cache(pageLikes, pathStr)(spooky)
  }

  //TODO: return option
  def restore(fullPath: Path)(spooky: SpookyContext): Seq[PageLike] = {

    val result = DFSRead("restore", fullPath.toString, spooky) {
      val fs = fullPath.getFileSystem(spooky.hConf)

      if (fs.exists(fullPath)) {

        val ser = SparkEnv.get.serializer.newInstance()
        val fis = fs.open(fullPath)
        val serIn = ser.deserializeStream(fis)
        try {
          serIn.readObject[Seq[PageLike]]()
        }
        finally{
          fis.close()
          serIn.close()
        }
      }
      else null
    }
    if (result!=null) spooky.metrics.pagesFetchedFromCache += result.count(_.isInstanceOf[Page])

    result
  }

  //restore latest in a directory
  //returns: Seq() => has backtrace dir but contains no page
  //returns null => no backtrace dir
  //TODO: cannot handle infinite duration, avoid using it!
  //TODO: return option
  def restoreLatest(
                     dirPath: Path,
                     earliestModificationTime: Long = 0
                     )(spooky: SpookyContext): Seq[PageLike] = {

    val latestStatus = DFSRead("get latest version", dirPath.toString, spooky) {

      val fs = dirPath.getFileSystem(spooky.hConf)

      if (fs.exists(dirPath) && fs.getFileStatus(dirPath).isDir) {

        val statuses = fs.listStatus(dirPath)

        statuses.filter(status => !status.isDir && status.getModificationTime >= earliestModificationTime)
          .sortBy(_.getModificationTime).lastOption
      }
      else None
    }

    latestStatus match {
      case Some(status) => restore(status.getPath)(spooky)
      case _ => null
    }
  }

  //TODO: return option
  def autoRestoreLatest(
                         trace: Trace,
                         spooky: SpookyContext
                         ): Seq[PageLike] = {
    val pathStr = Utils.urlConcat(
      spooky.autoCacheRoot,
      spooky.cacheTraceEncoder(trace).toString
    )

    val pages = restoreLatest(
      new Path(pathStr),
      System.currentTimeMillis() - spooky.pageExpireAfter.toMillis
    )(spooky)

    if (pages != null) for (page <- pages) {
      val pageBacktrace: Trace = page.uid.backtrace

      pageBacktrace.inject(trace.asInstanceOf[pageBacktrace.type])
      //this is to allow actions in backtrace to have different name than those cached
    }
    pages
  }
}
