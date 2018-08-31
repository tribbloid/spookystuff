package com.tribbloids.spookystuff.doc

import java.util.Date

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.utils.CommonUtils
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkEnv
import org.slf4j.LoggerFactory

object DocUtils {

  def dfsRead[T](message: String, pathStr: String, spooky: SpookyContext)(f: => T): T = {
    try {
      val result = CommonUtils.retry(Const.DFSLocalRetries) {
        CommonUtils.withDeadline(spooky.spookyConf.DFSTimeout) { f }
      }
      spooky.spookyMetrics.DFSReadSuccess += 1
      result
    } catch {
      case e: Throwable =>
        spooky.spookyMetrics.DFSReadFailure += 1
        val ex = new DFSReadException(pathStr, e)
        ex.setStackTrace(e.getStackTrace)
        if (spooky.spookyConf.failOnDFSError) throw ex
        else {
          LoggerFactory.getLogger(this.getClass).warn(message, ex)
          null.asInstanceOf[T]
        }
    }
  }

  //always fail on retry depletion and timeout
  def dfsWrite[T](message: String, pathStr: String, spooky: SpookyContext)(f: => T): T = {
    try {
      val result = CommonUtils.retry(Const.DFSLocalRetries) {
        CommonUtils.withDeadline(spooky.spookyConf.DFSTimeout) { f }
      }
      spooky.spookyMetrics.DFSWriteSuccess += 1
      result
    } catch {
      case e: Throwable =>
        spooky.spookyMetrics.DFSWriteFailure += 1
        val ex = new DFSWriteException(pathStr, e)
        ex.setStackTrace(e.getStackTrace)
        throw ex
    }
  }

  def load(pathStr: String)(spooky: SpookyContext): Array[Byte] =
    dfsRead("load", pathStr, spooky) {
      val result = spooky.pathResolver.input(pathStr) { in =>
        IOUtils.toByteArray(in.stream)
      }

      result
    }

  final val cacheVID = 91252374923L

  //unlike save, this will store all information in an unreadable, serialized, probably compressed file
  //always overwrite! use the same serializer as Spark
  def cache[T](
      pageLikes: Seq[T],
      pathStr: String,
      overwrite: Boolean = true
  )(spooky: SpookyContext): Unit =
    dfsWrite("cache", pathStr, spooky) {
      val list = pageLikes.toList // Seq may be a stream that cannot be serialized

      spooky.pathResolver.output(pathStr, overwrite) { out =>
        val ser = SparkEnv.get.serializer.newInstance()
        val serOut = ser.serializeStream(out.stream)

        try {
          serOut.writeObject(
            list.asInstanceOf[List[T] @SerialVersionUID(cacheVID) with Serializable]
          )
        } finally {
          serOut.close()
        }
      }
    }

  private def restore[T](pathStr: String)(spooky: SpookyContext): Seq[T] =
    dfsRead("restore", pathStr, spooky) {

      val result = spooky.pathResolver.input(pathStr) { in =>
        val ser = SparkEnv.get.serializer.newInstance()

        val serIn = ser.deserializeStream(in.stream)
        try {
          serIn.readObject[List[T]]()
        } finally {
          serIn.close()
        }
      }

      result
    }

  //  def autoCache(
  //                 pageLikes: Seq[Fetched],
  //                 spooky: SpookyContext
  //                 ): Unit = {
  //    val effectivePageLikes = pageLikes.filter(_.cacheable)
  //    if (effectivePageLikes.isEmpty) return
  //
  //    val pathStr = Utils.pathConcat(
  //      spooky.conf.dirs.cache,
  //      spooky.conf.cacheFilePath(effectivePageLikes.head.uid.backtrace).toString,
  //      UUID.randomUUID().toString
  //    )
  //
  //    cache(effectivePageLikes, pathStr)(spooky)
  //  }

  //restore latest in a directory
  //returns: Nil => has backtrace dir but contains no page
  //returns null => no backtrace dir
  def restoreLatest(
      dirPath: Path,
      earliestModificationTime: Long,
      latestModificationTime: Long
  )(spooky: SpookyContext): Seq[DocOption] = {

    val latestStatus: Option[FileStatus] = dfsRead("get latest version", dirPath.toString, spooky) {

      val fs = dirPath.getFileSystem(spooky.hadoopConf)

      if (fs.exists(dirPath) && fs.getFileStatus(dirPath).isDirectory) {

        val statuses = fs.listStatus(dirPath)

        statuses
        //          .filter(status => !status.isDirectory && status.getModificationTime >= earliestModificationTime - 300*1000) //Long enough for overhead of eventual consistency to take effect and write down file
          .filter(_.getModificationTime < latestModificationTime) //TODO: may have disk write delay!
          .sortBy(_.getModificationTime)
          .lastOption
      } else None
    }

    latestStatus match {
      case Some(status) =>
        val results = restore[DocOption](status.getPath.toString)(spooky)

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
                dirPath)
          null
        }
      case _ =>
        LoggerFactory.getLogger(this.getClass).info(s"Not cached: " + dirPath)
        null
    }
  }

  //TODO: return Option[Seq]
  //TODO: move into caching
  //  def autoRestore(
  //                   backtrace: Trace,
  //                   spooky: SpookyContext
  //                   ): Seq[Fetched] = {
  //
  //    import dsl._
  //
  //    val pathStr = Utils.pathConcat(
  //      spooky.conf.dirs.cache,
  //      spooky.conf.cacheFilePath(backtrace).toString
  //    )
  //
  //    val waybackOption = backtrace.last match {
  //      case w: Wayback =>
  //        Option(w.wayback).map{
  //          expr =>
  //            val result = expr.asInstanceOf[Literal[Long]].value
  //            spooky.conf.IgnoreDocsCreatedBefore match {
  //              case Some(date) =>
  //                assert(result > date.getTime, "SpookyConf.pageNotExpiredSince cannot be set to later than wayback date")
  //              case None =>
  //            }
  //            result
  //        }
  //      case _ =>
  //        None
  //    }
  //
  //    val nowMillis = waybackOption match {
  //      case Some(wayback) => wayback
  //      case None => System.currentTimeMillis()
  //    }
  //
  //    val earliestTime = spooky.conf.getEarliestDocCreationTime(nowMillis)
  //
  //    val latestTime = waybackOption.getOrElse(Long.MaxValue)
  //
  //    val pages = restoreLatest(
  //      new Path(pathStr),
  //      earliestTime,
  //      latestTime
  //    )(spooky)
  //
  //    if (pages != null) for (page <- pages) {
  //      val pageBacktrace: Trace = page.uid.backtrace
  //
  //      pageBacktrace.injectFrom(backtrace) //this is to allow actions in backtrace to have different name than those cached
  //    }
  //    pages
  //  }
}
