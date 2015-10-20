package com.tribbloids.spookystuff.sparkbinding

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.{SpookyEnvSuite, dsl}

/**
 * Created by peng on 5/10/15.
 */
class TestPageRowRDD extends SpookyEnvSuite {

  import dsl._

  test("should support repartition") {
    val spooky = this.spooky

    sc.setCheckpointDir(s"file://${System.getProperty("user.dir")}/temp/spooky-unit/${this.getClass.getSimpleName}/")

    val first = spooky
      .fetch(Wget("http://en.wikipedia.org")).persist()
    first.checkpoint()
    first.count()

    val second = first.wgetJoin(S"a".href, joinType = Append)
      .select(S.uri ~ 'uri)
      .repartition(14)
      .toJSON()

    val result = second.collect()
    result.foreach(println)

    assert(result.length == 2)
    assert(first.spooky.metrics.pagesFetched.value == 2)
  }

  test("toDF() should not run preceding transformation multiple times") {
    val acc = sc.accumulator(0)

    spooky
      .fetch(
        Wget("http://www.wikipedia.org/")
      )
      .select(
        S.andFlatMap{
          page =>
            acc += 1
            page.saved.headOption
        } ~ 'path
      )
      .toDF().count()

    assert(acc.value == 1) //TODO: should be 1: reduced to 1 after unpersistAfterRendering() implemented
  }

  test("toJSON() should not run preceding transformation multiple times") {
    val acc = sc.accumulator(0)

    spooky
      .fetch(
        Wget("http://www.wikipedia.org/")
      )
      .select(
        S.andFlatMap{
          page =>
            acc += 1
            page.saved.headOption
        } ~ 'path
      )
      .toJSON().count()

    assert(acc.value == 1)
  }

  test("toMapRDD() should not run preceding transformation multiple times") {
    val acc = sc.accumulator(0)

    spooky
      .fetch(
        Wget("http://www.wikipedia.org/")
      )
      .select(
        S.andFlatMap{
          page =>
            acc += 1
            page.saved.headOption
        } ~ 'path
      )
      .toMapRDD().count()

    assert(acc.value == 1)
  }
}