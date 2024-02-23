package com.tribbloids.spookystuff.commons

/**
  * @param iterators
  *   must have identical arities
  */
class InterleavedIterator[T](iterators: List[Iterator[T]]) extends Iterator[Seq[T]] {
  assert(iterators.nonEmpty, "iterator list is empty")

  def hasNext: Boolean = {
    val mapped = iterators.map(_.hasNext).distinct
    assert(mapped.length == 1, "iterators have different lengths")
    mapped.head
  }

  def next: List[T] = iterators.map(_.next)
}
