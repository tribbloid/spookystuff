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

  test("TraversableLike.filterByType should work on primitive types") {

    assert(Seq(1, 2.2, "a").filterByType[Int].get == Seq(1))
    assert(Seq(1, 2.2, "a").filterByType[java.lang.Integer].get == Seq(1: java.lang.Integer))
    assert(Seq(1, 2.2, "a").filterByType[Double].get == Seq(2.2))
    assert(Seq(1, 2.2, "a").filterByType[java.lang.Double].get == Seq(2.2: java.lang.Double))
    assert(Seq(1, 2.2, "a").filterByType[String].get == Seq("a"))
    
    assert(Set(1, 2.2, "a").filterByType[Int].get == Set(1))
    assert(Set(1, 2.2, "a").filterByType[java.lang.Integer].get == Set(1: java.lang.Integer))
    assert(Set(1, 2.2, "a").filterByType[Double].get == Set(2.2))
    assert(Set(1, 2.2, "a").filterByType[java.lang.Double].get == Set(2.2: java.lang.Double))
    assert(Set(1, 2.2, "a").filterByType[String].get == Set("a"))
  }

  test("Array.filterByType should work on primitive types") {

    assert(Array(1, 2.2, "a").filterByType[Int].toSeq == Seq(1))
    assert(Array(1, 2.2, "a").filterByType[java.lang.Integer].toSeq == Seq(1: java.lang.Integer))
    assert(Array(1, 2.2, "a").filterByType[Double].toSeq == Seq(2.2))
    assert(Array(1, 2.2, "a").filterByType[java.lang.Double].toSeq == Seq(2.2: java.lang.Double))
    assert(Array(1, 2.2, "a").filterByType[String].toSeq == Seq("a"))
  }

  test("1") {
    println(Seq("abc", "def", 3, 4, 2.3).filterByType[String].get)
    println(Seq("abc", "def", 3, 4, 2.3).filterByType[Integer].get)
    println(Seq("abc", "def", 3, 4, 2.3).filterByType[Int].get)
    println(Seq("abc", "def", 3, 4, 2.3).filterByType[java.lang.Double].get)
    println(Seq("abc", "def", 3, 4, 2.3).filterByType[Double].get)

    //    val res2: Array[String] = Array("abc", "def").filterByType[String].get
    //    println(res2)

    println(Set("abc", "def", 3, 4, 2.3).filterByType[String].get)
    println(Set("abc", "def", 3, 4, 2.3).filterByType[Integer].get)
    println(Set("abc", "def", 3, 4, 2.3).filterByType[Int].get)
    println(Seq("abc", "def", 3, 4, 2.3).filterByType[java.lang.Double].get)
    println(Seq("abc", "def", 3, 4, 2.3).filterByType[Double].get)
  }
}
