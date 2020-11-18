package org.apache.spark.rdd.spookystuff

import com.tribbloids.spookystuff.utils.lifespan.LifespanContext
import org.apache.spark.rdd.spookystuff.ExternalAppendOnlyArray.CannotComputeException
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
          s"Primary iterator ${_primary} cannot be created: $e"
        )
        logger.debug("", e)
        ConsumedIterator.empty
    }
  }

  @volatile var useBackup = false

  @transient final protected lazy val _backup: Iterator[T] with ConsumedIterator = {

    val raw = getBackup

    val difference = _primary.offset - raw.offset

    val result =
      if (difference < 0)
        throw new CannotComputeException(
          LifespanContext().toString + "\n" +
            s"In ${this.getClass}, backup ${raw} cannot go back: from ${raw.offset} to ${_primary.offset}"
        )
      else if (difference > 0) {

        raw.drop(difference)

      } else {
        raw
      }

//    LoggerFactory
//      .getLogger(this.getClass)
//      .info(
//        s"Synchronising back up iterator ${backup.getClass.getSimpleName} that is $difference steps behind"
//      )

    useBackup = true
//    _primary = null

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
          s"Primary iterator ${_primary} is broken at ${_primary.offset}, falling back to use ${_backup.getClass}\n" +
            s"caused by $e"
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
      _backup.hasNext
    }
  }

  final override def next(): T = {

    val result = primaryHasNext match {
      case Some(true) =>
        _primary.next()
      case Some(false) =>
        throw new UnsupportedOperationException("primary iterator has no more element")
      case None =>
        _backup.next()
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
