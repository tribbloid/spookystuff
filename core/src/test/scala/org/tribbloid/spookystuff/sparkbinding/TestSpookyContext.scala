package org.tribbloid.spookystuff.sparkbinding

import org.tribbloid.spookystuff.SpookyEnvSuite

/**
 * Created by peng on 18/07/15.
 */
class TestSpookyContext extends SpookyEnvSuite {

  test("SpookyContext should be Serializable") {

    val spooky = this.spooky
    val src = spooky.sqlContext.sparkContext.parallelize(1 to 10)

    val res = src.map {
      v => spooky.hashCode() + v
    }.reduce(_ + _)
  }

  test("SpookyContext.dsl should be Serializable") {

    val spooky = this.spooky
    val src = spooky.sqlContext.sparkContext.parallelize(1 to 10)

    val res = src.map {
      v => spooky.dsl.hashCode() + v
    }.reduce(_ + _)
  }
}