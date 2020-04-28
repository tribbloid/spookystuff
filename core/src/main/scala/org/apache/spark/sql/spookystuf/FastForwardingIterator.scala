package org.apache.spark.sql.spookystuf

trait FastForwardingIterator[T] extends Iterator[T] {

  def fastForward(n: Int): FastForwardingIterator[T]
}
