package org.apache.spark.rdd.spookystuff

trait FastForwardingIterator[T] extends Iterator[T] {

//  override def drop(n: Int): Iterator[T]

  final override def drop(n: Int): this.type = fastForward(n)

  protected def fastForward(n: Int): this.type
}
