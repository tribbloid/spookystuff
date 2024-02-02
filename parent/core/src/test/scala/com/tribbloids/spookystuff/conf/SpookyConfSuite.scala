package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.testutils.SpookyBaseSpec
import com.tribbloids.spookystuff.utils.ConfUtils
import com.tribbloids.spookystuff.utils.serialization.AssertSerializable
import org.apache.spark.SparkConf

import scala.util.Random

/**
  * Created by peng on 30/09/16.
  */
class SpookyConfSuite extends SpookyBaseSpec {

  def conf = new SpookyConf()
  def dirConf: Dir.Conf = Dir.Conf()

  it("SpookyConf is serializable") {

    AssertSerializable(
      conf,
      condition = { (v1: SpookyConf, v2: SpookyConf) =>
        v1.cacheFileStructure == v2.cacheFileStructure
      }
    )
  }

  it("DirConf.import is serializable") {

    val sparkConf = new SparkConf()
    val dummyV = "dummy" + Random.nextLong()
    sparkConf.set("spooky.dirs.auditing", dummyV)
    val imported = dirConf.importFrom(sparkConf)
    AssertSerializable(
      imported
    )
  }

  it("DirConf.import can read from SparkConf") {
    val sparkConf = new SparkConf()
    val dummyV = "dummy" + Random.nextLong()
    sparkConf.set("spooky.dirs.auditing", dummyV)
    val imported = dirConf.importFrom(sparkConf)

    assert(imported.auditing == dummyV)
  }

  it("getProperty() can load property from spark property") {
    val conf = sc.getConf
    val v = conf.get("dummy.property")

    assert(ConfUtils.getPropertyOrEnv("dummy.property").contains(v))
  }

  it("getProperty() can load property from system property") {

    System.setProperty("dummy.property", "AA")

    try {
      assert(ConfUtils.getPropertyOrEnv("dummy.property").contains("AA"))
    } finally {
      System.setProperty("dummy.property", "")
    }
  }
}
