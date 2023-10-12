package com.tribbloids.spookystuff.testutils

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.conf._

trait SpookyEnvSpec extends SparkEnvSpec {

  var _spooky: SpookyContext = _
  def spooky: SpookyContext = {
    Option(_spooky)
      .getOrElse {
        val result: SpookyContext = reloadSpooky
        result
      }
  }

  def reloadSpooky: SpookyContext = {
    val sql = this.sql
    val result = SpookyContext(sql, SpookyConf.default)
    _spooky = result
    result
  }

  def spookyConf: SpookyConf = spooky.getConf(Core)
}

object SpookyEnvSpec {}
