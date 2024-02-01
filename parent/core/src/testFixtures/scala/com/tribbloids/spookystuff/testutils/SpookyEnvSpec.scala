package com.tribbloids.spookystuff.testutils

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.conf._

trait SpookyEnvSpec extends SparkEnvSpec {

  var _ctxOverride: SpookyContext = _

  def getDefaultCtx: SpookyContext = {
    val sql = this.sql
    val result = SpookyContext(sql, SpookyConf.default)
//    _ctxOverride = result
    result
  }
  final lazy val defaultCtx = getDefaultCtx

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

object SpookyEnvSpec {}
