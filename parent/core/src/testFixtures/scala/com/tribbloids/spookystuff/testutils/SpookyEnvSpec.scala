package com.tribbloids.spookystuff.testutils

import ai.acyclic.prover.commons.spark.{SparkEnvSpec, TestHelper}
import ai.acyclic.prover.commons.util.Magnet.OptionMagnet
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.conf._

trait SpookyEnvSpec extends BaseSpec with SparkEnvSpec {

  def _ctxOverride: OptionMagnet[SpookyContext] = None

  lazy val defaultCtx: SpookyContext = SpookyEnvSpec.defaultCtx

  final def spooky: SpookyContext = {
    _ctxOverride.revoke
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
