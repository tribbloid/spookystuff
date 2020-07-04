package org.apache.spark.rdd.spookystuf

import org.apache.spark.rdd.spookystuf.ExternalAppendOnlyArray.CannotComputeException
import org.slf4j.LoggerFactory

/**
  * use primary until it is drained or broken, then use backup
  * if primary cannot be created then use backup directly
  * will discard primary immediately once backup is taking over
  * @tparam T type of element
  */
trait FallbackIterator[T] extends FastForwardingIterator[T] with ConsumedIterator {

  def getPrimary: Iterator[T] with ConsumedIterator
  def getBackup: Iterator[T] with ConsumedIterator

  @transient @volatile final protected var _primary: Iterator[T] with ConsumedIterator = {
    try {
      getPrimary
    } catch {
      case e: Throwable =>
        val logger = LoggerFactory.getLogger(this.getClass)

        logger.error(
          s"Primary iterator ${_primary} cannot be created: ${e}"
        )
        logger.debug("", e)
        ConsumedIterator.empty
    }
  }

  final protected lazy val _backup: Iterator[T] with ConsumedIterator = getBackup
  @volatile var useBackup = false

  lazy val fallbackCachingUpOnce: Iterator[T] = {

    val difference = _primary.offset - _backup.offset

    val result =
      if (difference < 0)
        throw new CannotComputeException(
          s"fallback iterator cannot go back: from ${_backup.offset} to ${_primary.offset}"
        )
      else if (difference > 0) {

        _backup.drop(difference)

      } else {
        _backup
      }

//    LoggerFactory
//      .getLogger(this.getClass)
//      .info(
//        s"Synchronising back up iterator ${backup.getClass.getSimpleName} that is $difference steps behind"
//      )

    useBackup = true
    _primary = null

    result
  }

  final override def offset: Int = {
    if (useBackup) {
      _backup.offset
    } else {
      _primary.offset
    }
  }

  /**
    * @return has 3 values:
    * - Some(true): use primary as it still has more item
    * - Some(false): terminate immediately
    * - None: backup should take over
    */
  protected def _primaryHasNext: Option[Boolean] = {

    try {
      if (_primary.hasNext) Some(true)
      else None
    } catch {
      case e: Throwable =>
        val logger = LoggerFactory.getLogger(this.getClass)

        logger.error(
          s"Primary iterator ${_primary} is broken at ${_primary.offset}: $e"
        )
        logger.debug("", e)

        None
    }
  }

  final protected def primaryHasNext: Option[Boolean] = {

    if (useBackup) None
    else _primaryHasNext
  }

  final override def hasNext: Boolean = {
    primaryHasNext.getOrElse {
      fallbackCachingUpOnce.hasNext
    }
  }

  final override def next(): T = {

    val result = primaryHasNext match {
      case Some(true) =>
        _primary.next()
      case Some(false) =>
        throw new UnsupportedOperationException("primary iterator has no more element")
      case None =>
        fallbackCachingUpOnce.next()
    }

    result
  }

  final override protected def fastForward(n: Int): this.type = {

    primaryHasNext match {
      case Some(_) =>
        _primary.drop(n)
      case None =>
        _backup.drop(n)
    }

    this
  }
}

object FallbackIterator {}
