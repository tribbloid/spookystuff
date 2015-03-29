package org.tribbloid.spookystuff

import org.tribbloid.spookystuff.actions._
import dsl._

/**
 * Created by peng on 3/29/15.
 */
class TestSpookyContext extends SpookyEnvSuite{

  test("each noInput should have independent metrics if sharedMetrics=false") {

    val spooky = this.spooky
    spooky.conf.sharedMetrics = false

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
    spooky.conf.sharedMetrics = true

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

    assert(rdd1.spooky.metrics === rdd2.spooky.metrics)
  }
}
