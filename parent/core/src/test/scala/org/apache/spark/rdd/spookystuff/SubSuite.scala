package org.apache.spark.rdd.spookystuff

import org.scalatest.Suite

trait SubSuite extends Suite with Product {

  override lazy val suiteId: String = this.getClass.getName + " - " + productIterator.toList.mkString("-")

  override def suiteName: String = suiteId
}
