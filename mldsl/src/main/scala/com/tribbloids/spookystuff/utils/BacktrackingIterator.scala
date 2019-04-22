package com.tribbloids.spookystuff.utils

import scala.collection.mutable.ArrayBuffer

/**
  * heavily stateful iterator that can revert to any previous state using time machine
  * currently, maxBacktracking is forced to infinite, which consumes huge amount of memory
  * this will need further optimisation if streaming support is on the map
  */
case class BacktrackingIterator[T](
    self: Iterator[T],
    maxBacktracking: Int = -1
) extends Iterator[T] {

  val history: ArrayBuffer[T] = ArrayBuffer.empty

  def historySansCurrent: Seq[T] = history.init

  @volatile var _backtracking: Int = -1

  def backtracking: Int = {
    _backtracking
  }

  def backtracking_=(v: Int): Unit = {

    if (v >= history.length) _backtracking = -1
    else _backtracking = v
  }
//
//  def scanned: ArrayBuffer[T] = preceding :+ current

  override def hasNext: Boolean = {
    (backtracking >= 0) || self.hasNext
  }

  override def next(): T = {

    if (backtracking >= 0) {
      val result = history(backtracking)
      backtracking += 1

      result
    } else {

      val v = self.next()
      history += v
      v
    }
  }

  val checkpoints: ArrayBuffer[Int] = ArrayBuffer.empty

  def snapshot(): Int = {
    checkpoints += history.length
    checkpoints.size - 1
  }

  def revert(i: Int): Unit = {
    assert(i <= checkpoints.length, s"index $i exceeds number of checkpoints ${checkpoints.size} - 1")
    val checkpoint = checkpoints(i)
    _backtracking = checkpoint
  }
}
