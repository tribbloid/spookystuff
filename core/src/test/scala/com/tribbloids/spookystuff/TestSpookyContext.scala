package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.actions._
import dsl._

/**
 * Created by peng on 3/29/15.
 */
class TestSpookyContext extends SpookyEnvSuite{

  test("derived instances of a SpookyContext should have the same configuration") {

    val spooky = this.spooky
    spooky.conf.shareMetrics = false
    spooky.conf.dirs.cache = "dog"

    val rdd2 = spooky.create(Seq("dummy"))
    assert(!(rdd2.spooky eq spooky))

    val conf1 = spooky.conf.dirs.toJSON
    val conf2 = rdd2.spooky.conf.dirs.toJSON
    assert(conf1 == conf2)
  }

  test("derived instances of a SpookyContext should have the same configuration after it has been modified") {

    val spooky = this.spooky
    spooky.conf.shareMetrics = false
    spooky.conf.dirs.root = "s3://root"
    spooky.conf.dirs.cache = "hdfs://dummy"

    val rdd2 = spooky.create(Seq("dummy"))
    assert(!(rdd2.spooky eq spooky))

    val conf1 = spooky.conf.dirs.toJSON
    val conf2 = rdd2.spooky.conf.dirs.toJSON
    assert(conf1 == conf2)
  }

  test("each noInput should have independent metrics if sharedMetrics=false") {

    val spooky = this.spooky
    spooky.conf.shareMetrics = false

    val rdd1 = spooky
      .fetch(
        Wget("http://www.wikipedia.org")
      )
    rdd1.count()

    val rdd2 = spooky
      .fetch(
        Wget("http://en.wikipedia.org")
      )
    rdd2.count()

    assert(rdd1.spooky.metrics !== rdd2.spooky.metrics)
    assert(rdd1.spooky.metrics.pagesFetched.value === 1)
    assert(rdd2.spooky.metrics.pagesFetched.value === 1)
  }

  test("each noInput should have shared metrics if sharedMetrics=true") {

    val spooky = this.spooky
    spooky.conf.shareMetrics = true

    val rdd1 = spooky
      .fetch(
        Wget("http://www.wikipedia.org")
      )
    rdd1.count()

    val rdd2 = spooky
      .fetch(
        Wget("http://en.wikipedia.org")
      )
    rdd2.count()

    assert(rdd1.spooky.metrics.toJSON === rdd2.spooky.metrics.toJSON)
  }
}
