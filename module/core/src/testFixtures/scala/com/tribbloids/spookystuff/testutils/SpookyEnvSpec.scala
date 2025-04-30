package com.tribbloids.spookystuff.testutils

import ai.acyclic.prover.commons.spark.{SparkEnvSpec, TestHelper}
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.conf._

trait SpookyEnvSpec extends BaseSpec with SparkEnvSpec {

  @transient final lazy val spooky: SpookyContext = {
    SpookyEnvSpec.defaultCtx
  }

  def spookyConf: SpookyConf = spooky(Core).conf
}

object SpookyEnvSpec {

  lazy val defaultCtx: SpookyContext = {
    val sql = TestHelper.TestSQL
    val result = SpookyContext(sql, SpookyConf.default)
    //    _ctxOverride = result
    result
  }
}
