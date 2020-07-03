package org.apache.spark.rdd.spookystuf

import org.scalatest.FunSpec

class FallbackIteratorSuite extends FunSpec {

  it("can consume from 1 iterator") {

    val src = (1 to 100).iterator

    val itr = new FallbackIterator[Int] {
      override def getPrimary: Iterator[Int] with ConsumedIterator = src

      override def getBackup: Iterator[Int] with ConsumedIterator = src
    }

    val result = itr.toList

    assert(result === (1 to 100))
  }

  it("can consume from 2 iterators") {

    val src1, src2 = (1 to 100).iterator

    val itr = new FallbackIterator[Int] {
      override def getPrimary: Iterator[Int] with ConsumedIterator = src1

      override def getBackup: Iterator[Int] with ConsumedIterator = src2
    }

    val result = itr.toList

    assert(result === (1 to 100))
  }
}
