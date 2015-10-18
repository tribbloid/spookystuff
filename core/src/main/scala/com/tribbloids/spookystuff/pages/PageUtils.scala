package com.tribbloids.spookystuff.pages

import java.util.{Date, UUID}

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.actions.{Trace, Wayback}
import com.tribbloids.spookystuff.expressions.Literal
import com.tribbloids.spookystuff.utils.{HDFSResolver, Serializable, Utils}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkEnv
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration.Infinite

/**
 * Created by peng on 11/27/14.
 */
object PageUtils {

  def dfsRead[T](message: String, pathStr: String, spooky: SpookyContext)(f: => T): T = {
    try {
      val result = Utils.retry(Const.DFSLocalRetries) {
        Utils.withDeadline(spooky.conf.DFSTimeout) {f}
      }
      spooky.metrics.DFSReadSuccess += 1
      result
    }
    catch {
      case e: Throwable =>
        spooky.metrics.DFSReadFailure += 1
        val ex = new DFSReadException(pathStr ,e)
        ex.setStackTrace(e.getStackTrace)
        if (spooky.conf.failOnDFSError) throw ex
        else {
          LoggerFactory.getLogger(this.getClass).warn(message, ex)
          null.asInstanceOf[T]
        }
    }
  }

  //always fail on retry depletion and timeout
  def dfsWrite[T](message: String, pathStr: String, spooky: SpookyContext)(f: => T): T = {
    try {
      val result = Utils.retry(Const.DFSLocalRetries) {
        Utils.withDeadline(spooky.conf.DFSTimeout) {f}
      }
      spooky.metrics.DFSWriteSuccess += 1
      result
    }
    catch {
      case e: Throwable =>
        spooky.metrics.DFSWriteFailure += 1
        val ex = new DFSWriteException(pathStr ,e)
        ex.setStackTrace(e.getStackTrace)
        throw ex
    }
  }

  def load(pathStr: String)(spooky: SpookyContext): Array[Byte] =
    dfsRead("load", pathStr, spooky) {
      val result = HDFSResolver(spooky.hadoopConf).input(pathStr) {
        fis =>
          IOUtils.toByteArray(fis)
      }

      result
    }

  //unlike save, this will store all information in an unreadable, serialized, probably compressed file
  //always overwrite! use the same serializer as Spark
  private def cache[T](
                        pageLikes: Seq[T],
                        pathStr: String,
                        overwrite: Boolean = true
                        )(spooky: SpookyContext): Unit =
    dfsWrite("cache", pathStr, spooky) {
      val resolver = HDFSResolver(spooky.hadoopConf)

      resolver.output(pathStr, overwrite) {
        fos =>
          val ser = SparkEnv.get.serializer.newInstance()
          val serOut = ser.serializeStream(fos)

          try {
            serOut.writeObject[Seq[T]](Serializable[Seq[T]](pageLikes, 91252374923L))
          }
          finally {
            serOut.close()
          }
      }
    }

  private def restore[T](pathStr: String)(spooky: SpookyContext): Seq[T] =
    dfsRead("restore", pathStr, spooky) {

      val result = HDFSResolver(spooky.hadoopConf).input(pathStr) {
        fis =>
          val ser = SparkEnv.get.serializer.newInstance()

          val serIn = ser.deserializeStream(fis)
          try {
            serIn.readObject[Seq[T]]()
          }
          finally{
            serIn.close()
          }
      }

      result
    }

  def autoCache(
                 pageLikes: Seq[PageLike],
                 spooky: SpookyContext
                 ): Unit = {
    val effectivePageLikes = pageLikes.filter(_.cacheable)
    if (effectivePageLikes.isEmpty) return

    val pathStr = Utils.uriConcat(
      spooky.conf.dirs.cache,
      spooky.conf.cachePath(effectivePageLikes.head.uid.backtrace).toString,
      UUID.randomUUID().toString
    )

    cache(effectivePageLikes, pathStr)(spooky)
  }

  //restore latest in a directory
  //returns: Nil => has backtrace dir but contains no page
  //returns null => no backtrace dir
  private def restoreLatest(
                             dirPath: Path,
                             earliestModificationTime: Long,
                             latestModificationTime: Long
                             )(spooky: SpookyContext): Seq[PageLike] = {

    val latestStatus: Option[FileStatus] = dfsRead("get latest version", dirPath.toString, spooky) {

      val fs = dirPath.getFileSystem(spooky.hadoopConf)

      if (fs.exists(dirPath) && fs.getFileStatus(dirPath).isDirectory) {

        val statuses = fs.listStatus(dirPath)

        statuses
          //          .filter(status => !status.isDirectory && status.getModificationTime >= earliestModificationTime - 300*1000) //Long enough for overhead of eventual consistency to take effect and write down file
          .filter(_.getModificationTime < latestModificationTime) //TODO: may have disk write delay!
          .sortBy(_.getModificationTime).lastOption
      }
      else None
    }

    latestStatus match {
      case Some(status) =>
        val results = restore[PageLike](status.getPath.toString)(spooky)

        if (results == null) {
          LoggerFactory.getLogger(this.getClass).warn("Cached content is corrputed")
          null
        }
        else if (results.head.timestamp.getTime >= earliestModificationTime) results
        else {
          LoggerFactory.getLogger(this.getClass).info(s"All cached contents has become obsolete after ${new Date(earliestModificationTime).toString}:\n" +
            s"$dirPath")
          null
        }
      case _ =>
        LoggerFactory.getLogger(this.getClass).info(s"Not cached:\n" +
          s"$dirPath")
        null
    }
  }

  //TODO: return option
  def autoRestore(
                   backtrace: Trace,
                   spooky: SpookyContext
                   ): Seq[PageLike] = {

    import dsl._

    val pathStr = Utils.uriConcat(
      spooky.conf.dirs.cache,
      spooky.conf.cachePath(backtrace).toString
    )

    val waybackOption = backtrace.last match {
      case w: Wayback =>
        Option(w.wayback).map{
          expr =>
            val result = expr.asInstanceOf[Literal[Long]].value
            spooky.conf.pageNotExpiredSince match {
              case Some(date) =>
                assert(result > date.getTime, "SpookyConf.pageNotExpiredSince cannot be set to later than wayback date")
              case None =>
            }
            result
        }
      case _ =>
        None
    }

    val earliestTimeFromDuration = spooky.conf.pageExpireAfter match {
      case inf: Infinite => Long.MinValue
      case d =>
        waybackOption match {
          case Some(wayback) => wayback - d.toMillis
          case None => System.currentTimeMillis() - d.toMillis
        }
    }
    val earliestTime = spooky.conf.pageNotExpiredSince match {
      case Some(expire) =>
        Math.max(expire.getTime, earliestTimeFromDuration)
      case None =>
        earliestTimeFromDuration
    }

    val latestTime = waybackOption.getOrElse(Long.MaxValue)

    val pages = restoreLatest(
      new Path(pathStr),
      earliestTime,
      latestTime
    )(spooky)

    if (pages != null) for (page <- pages) {
      val pageBacktrace: Trace = page.uid.backtrace

      pageBacktrace.injectFrom(backtrace) //this is to allow actions in backtrace to have different name than those cached
    }
    pages
  }
}
