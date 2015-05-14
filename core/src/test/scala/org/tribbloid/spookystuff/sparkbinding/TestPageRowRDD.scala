package org.tribbloid.spookystuff.sparkbinding

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.{SpookyEnvSuite, dsl}

/**
 * Created by peng on 5/10/15.
 */
class TestPageRowRDD extends SpookyEnvSuite {

  import dsl._

  test("should support repartition") {
    val spooky = this.spooky

    sc.setCheckpointDir(s"file://${System.getProperty("user.home")}/spooky-local/${this.getClass.getSimpleName}/")

    val first = spooky
      .fetch(Wget("http://en.wikipedia.org")).persist()
    first.checkpoint()
    first.count()
    first.unpersist()

    val second = first.wgetJoin($"a".href, joinType = Append)
      .select($.uri ~ 'uri)
      .repartition(14)
      .toJSON()

    val result = second.collect()
    result.foreach(println)

    assert(result.length == 2)
    assert(first.spooky.metrics.pagesFetched.value == 2)
  }
}