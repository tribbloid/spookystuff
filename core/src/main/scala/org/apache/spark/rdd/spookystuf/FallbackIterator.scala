package org.apache.spark.rdd.spookystuf

import org.apache.spark.rdd.spookystuf.ExternalAppendOnlyArray.CannotComputeException
import org.slf4j.LoggerFactory

trait FallbackIterator[T] extends FastForwardingIterator[T] with ConsumedIterator {

  // use primary until it is drained, then use fallback
  //  and will discard primary immediately
  var primary: Iterator[T] with ConsumedIterator
  def backup: Iterator[T] with ConsumedIterator

  @transient var useBackup = false

  lazy val fallbackCachingUpOnce: Iterator[T] = {

    val difference = primary.offset - backup.offset

    val result =
      if (difference < 0)
        throw new CannotComputeException(
          s"fallback iterator can't go back: from ${backup.offset} to ${primary.offset}"
        )
      else if (difference > 0) {

        backup.drop(difference)

      } else {
        backup
      }

    LoggerFactory
      .getLogger(this.getClass)
      .info(
        s"Synchronising back up iterator ${backup.getClass.getSimpleName} that is $difference steps behind"
      )
    useBackup = true
    primary = ConsumedIterator.empty

    result
  }

  final override def offset: Int = {
    if (useBackup) {
      backup.offset
    } else {
      primary.offset
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
      if (primary.hasNext) Some(true)
      else None
    } catch {
      case _: Throwable =>
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
        primary.next()
      case Some(false) =>
        throw new UnsupportedOperationException("primary iterator has no more element")
      case None =>
        fallbackCachingUpOnce.next()
    }

    result
  }

  final override def drop(n: Int): this.type = {

    primaryHasNext match {
      case Some(_) =>
        primary.drop(n)
      case None =>
        backup.drop(n)
    }

    this
  }
}

object FallbackIterator {}
