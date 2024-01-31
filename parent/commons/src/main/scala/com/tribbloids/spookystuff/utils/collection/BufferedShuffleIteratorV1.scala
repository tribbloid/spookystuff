package com.tribbloids.spookystuff.utils.collection

import ai.acyclic.prover.commons.util.Caching

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object BufferedShuffleIteratorV1 {

  /**
    * (instanceID -> partitionID) -> iterator
    */
  val seeds: Caching.ConcurrentMap[(String, Int), Long] = Caching.ConcurrentMap[(String, Int), Long]()
}

case class BufferedShuffleIteratorV1[T](
    dep: Iterator[T],
    cap: Int = 100,
    seed: Long = Random.nextLong()
) extends Iterator[T] {

  @transient lazy val buffer: ArrayBuffer[T] = new ArrayBuffer[T](cap + 1)

  @transient lazy val random: Random = {

    new Random(seed)
  }

//  var nextCounter = 0

  override def hasNext: Boolean = dep.hasNext || buffer.nonEmpty

  def popRandom(): T = {
    val size = buffer.size
    val randomIndex = random.nextInt(size)
//    nextCounter += 1
//    println(s"--instance=$instanceID \t--key=${group -> TaskContext.getPartitionId()} \t--seed=$seed \t--counter=$nextCounter \t--i=$randomIndex")
    val result = buffer(randomIndex)
    buffer.update(randomIndex, buffer(size - 1))
    buffer.trimEnd(1)
    result
  }

  def push(v: T): Unit = {
    buffer += v
  }

  override def next(): T = {

    val size = buffer.size
    while (size <= cap && dep.hasNext) push(dep.next())

    val result = popRandom()
    result
  }
}
