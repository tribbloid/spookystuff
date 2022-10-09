package org.apache.spark.rdd.spookystuff

import org.scalatest.Suite

import java.util.concurrent.atomic.AtomicInteger

trait SubSuite extends Suite with Product {

  override def suiteId: String =
    (this.getClass.getName +: productIterator.toList :+ SubSuite.incrementalSuffixes.getAndIncrement())
      .mkString("-")

  override def suiteName: String = suiteId
}

object SubSuite {

  val incrementalSuffixes = new AtomicInteger(0)
}
