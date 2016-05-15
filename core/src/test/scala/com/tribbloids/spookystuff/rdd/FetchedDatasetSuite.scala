package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.{SpookyEnvSuite, dsl}

/**
  * Created by peng on 5/10/15.
  */
class FetchedDatasetSuite extends SpookyEnvSuite {

  import dsl._

//    test("should support repartition") {
//      val spooky = this.spooky
//
//      sc.setCheckpointDir(s"file://${System.getProperty("user.dir")}/temp/spooky-unit/${this.getClass.getSimpleName}/")
//
//      val first = spooky
//        .fetch(Wget("http://en.wikipedia.org")).persist()
//      first.checkpoint()
//      first.count()
//
//      val second = first.wgetJoin(S"a".hrefs, joinType = LeftOuter)
//        .extract(S.uri ~ 'uri)
//        .repartition(14)
//
//      val result = second.collect()
//      result.foreach(println)
//
//      assert(result.length == 2)
//      assert(first.spooky.metrics.pagesFetched.value == 2)
//    }

  for (sort <- Seq(false, true)) {
    test(s"toDF($sort) should not run preceding transformation multiple times") {
      val acc = sc.accumulator(0)

      spooky
        .fetch(
          Wget("http://www.wikipedia.org/")
        )
        .extract(
          S.andOptional{
            page =>
              acc += 1
              page.saved.headOption
          } ~ 'path
        )
        .toDF(sort).count()

      assert(acc.value == 1)
    }

    test(s"toJSON($sort) should not run preceding transformation multiple times") {
      val acc = sc.accumulator(0)

      spooky
        .fetch(
          Wget("http://www.wikipedia.org/")
        )
        .extract(
          S.andOptional{
            page =>
              acc += 1
              page.saved.headOption
          } ~ 'path
        )
        .toJSON(sort).count()

      assert(acc.value == 1)
    }

    test(s"toMapRDD($sort) should not run preceding transformation multiple times") {
      val acc = sc.accumulator(0)

      spooky
        .fetch(
          Wget("http://www.wikipedia.org/")
        )
        .select(
          S.andOptional{
            page =>
              acc += 1
              page.saved.headOption
          } ~ 'path
        )
        .toMapRDD(sort).count()

      assert(acc.value == 1)
    }
  }
}