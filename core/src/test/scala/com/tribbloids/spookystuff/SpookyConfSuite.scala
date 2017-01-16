package com.tribbloids.spookystuff

import org.apache.spark.SparkConf

import scala.util.Random

/**
  * Created by peng on 30/09/16.
  */
class SpookyConfSuite extends SpookyEnvFixture {

  def conf = new SpookyConf()
  test("SpookyConf is serializable") {

    assertSerializable(
      conf,
      condition = {
        (v1: SpookyConf, v2: SpookyConf) =>
          v1.submodules.mkString("\n").shouldBe (
            v2.submodules.mkString("\n")
          )
      }
    )
  }

  test("SpookyConf.import is serializable") {
    val imported = conf.importFrom(sc.getConf)
    assertSerializable(
      imported,
      condition = {
        (v1: SpookyConf, v2: SpookyConf) =>
          v1.submodules.mkString("\n").shouldBe (
            v2.submodules.mkString("\n")
          )
      }
    )
  }

  test("SpookyConf.import can read from SparkConf before any of its submodule is created") {
    val sparkConf = new SparkConf()
    val dummyV = "dummy" + Random.nextLong()
    sparkConf.set("spooky.dirs.autosave", dummyV)
    val imported = conf.importFrom(sparkConf)
    val dirConf = imported.dirConf
    assert(dirConf.autoSave == dummyV)
  }

//  test("getProperty() can load property from system environment") {
//  }

  test("getProperty() can load property from system property") {

    System.setProperty("dummy.property", "AA")

    try {
      assert(AbstractConf.get("dummy.property") == Some("AA"))
    }
    finally {
      System.setProperty("dummy.property", "")
    }
  }

  test("getProperty() can load property from spark property") {
    System.setProperty("dummy.property", "AA")
    val conf = sc.getConf
    conf.set("dummy.property", "BB")

    try {
      assert(AbstractConf.get("dummy.property") == Some("BB"))
    }
    finally {
      conf.remove("dummy.property")
      System.setProperty("dummy.property", "")
    }
  }
}
