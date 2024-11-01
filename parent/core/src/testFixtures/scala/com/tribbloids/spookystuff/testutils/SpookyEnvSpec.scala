package com.tribbloids.spookystuff.testutils

import ai.acyclic.prover.commons.spark.{SparkEnvSpec, TestHelper}
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.conf._

trait SpookyEnvSpec extends SparkEnvSpec {

  var _ctxOverride: SpookyContext = _

  lazy val defaultCtx: SpookyContext = SpookyEnvSpec.defaultCtx

  final def spooky: SpookyContext = {
    Option(_ctxOverride)
      .getOrElse {
        val result: SpookyContext = defaultCtx
        result
      }
  }

  def spookyConf: SpookyConf = spooky(Core).conf

//  def withConf[T](modify: SpookyConf => SpookyConf)(fn: SpookyContext => T): T = {
//
//    val oldConf = spooky.getConf(Core)
//    val newConf = modify(oldConf)
//
//    val result =
//      try {
//
//        spooky.setConf(newConf)
//        fn(spooky)
//      } finally {
//        spooky.setConf(oldConf)
//      }
//
//    result
//  }
}

object SpookyEnvSpec {

  lazy val defaultCtx: SpookyContext = {
    val sql = TestHelper.TestSQL
    val result = SpookyContext(sql, SpookyConf.default)
    //    _ctxOverride = result
    result
  }
}
