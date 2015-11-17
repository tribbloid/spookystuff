package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.SpookyEnvSuite

import scala.util.Random

/**
  * Created by peng on 16/11/15.
  */
class TestViews extends SpookyEnvSuite {

  import Views._

  test("multiPassFlatMap should yield same result as flatMap") {

    val src = sc.parallelize(1 to 100).persist()

    val counter = sc.accumulator(0)
    val counter2 = sc.accumulator(0)

    val res1 = src.flatMap(v => Seq(v, v*2, v*3))
    val res2 = src.multiPassFlatMap{
      v =>
        val rand = Random.nextBoolean()
        counter2 +=1
        if (rand) {
          counter +=1
          Some(Seq(v, v*2, v*3))
        }
        else None
    }

    assert(res1.collect().toSeq == res2.collect().toSeq)
    assert(counter.value == 100)
    assert(counter2.value > 100)
  }
}
