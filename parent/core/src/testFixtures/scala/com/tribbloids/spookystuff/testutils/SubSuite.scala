package com.tribbloids.spookystuff.testutils

import org.scalatest.Suite

import java.util.concurrent.atomic.AtomicInteger

trait SubSuite extends Suite {

  private lazy val _class: Class[_ <: SubSuite] = this.getClass

  private lazy val _idParts: List[String] = {
    List(_class.getName)
  }

  override lazy val suiteName: String = _class.getName.stripPrefix(_class.getPackage.getName).stripPrefix(".")

  private lazy val _suiteId: String = {
    val result = (_idParts :+ SubSuite.incrementalSuffixes.getAndIncrement())
      .mkString("-")
    result
  }

  override def suiteId: String = {
    _suiteId
  }
}

object SubSuite {

  val incrementalSuffixes: AtomicInteger = new AtomicInteger(0)
}
