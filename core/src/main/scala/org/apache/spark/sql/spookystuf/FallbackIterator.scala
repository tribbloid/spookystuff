package org.apache.spark.sql.spookystuf

import org.apache.spark.sql.spookystuf.ExternalAppendOnlyArray.CannotComputeException
import org.slf4j.LoggerFactory

trait FallbackIterator[T] extends FastForwardingIterator[T] with ConsumedIterator {

  // use primary until it is drained, then use fallback
  //  and will discard primary immediately
  var primary: Iterator[T] with ConsumedIterator
  def backup: Iterator[T] with ConsumedIterator

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
        s"Synchronising back up iterator ${backup.getClass.getSimpleName} that is ${difference} steps behind"
      )

    primary = ConsumedIterator.empty

    result
  }

  override def offset: Int = Math.max(primary.offset, backup.offset)

  override def hasNext: Boolean = {
    primary.hasNext || {
      fallbackCachingUpOnce.hasNext
    }
  }

  override def next(): T = {

    val result = if (primary.hasNext) {
      primary.next()
    } else {
      val result = fallbackCachingUpOnce.next()
      //          println("compute:" + result)

      result
    }

    result
  }

  final override def drop(n: Int): this.type = {

    if (primary.hasNext) primary.drop(n)
    else backup.drop(n)

    this
  }
}

object FallbackIterator {}
