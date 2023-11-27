package com.tribbloids.spookystuff.testutils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.BeforeAndAfterAll

trait SparkEnvSpec extends BaseSpec with BeforeAndAfterAll with SparkUISupport {

  final def sc: SparkContext = TestHelper.TestSC

  final def sql: SQLContext = TestHelper.TestSQL

  final def parallelism: Int = sc.defaultParallelism

  override def beforeAll(): Unit = {

    super.beforeAll()
    sc // initialize before all tests
  }
}
