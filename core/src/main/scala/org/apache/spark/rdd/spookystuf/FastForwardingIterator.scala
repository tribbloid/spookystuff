package org.apache.spark.rdd.spookystuf

trait FastForwardingIterator[T] extends Iterator[T] {

//  override def drop(n: Int): Iterator[T]

  abstract override def drop(n: Int): this.type = ???
}
