package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.{Metrics, SpookyEnvFixture}
import com.tribbloids.spookystuff.testbeans._
import com.tribbloids.spookystuff.testutils.TestHelper
import com.tribbloids.spookystuff.utils.lifespan.LifespanContext
import com.tribbloids.spookystuff.utils.locality.PartitionIdPassthrough
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkEnv, TaskContext}

import scala.util.Random

object SpookyViewsSuite {

  val getThreadInfo = {
    () =>
      Option(TaskContext.get()).foreach {
        tc =>
          TestHelper.assert(!tc.isRunningLocally())
      }
      val ctx = LifespanContext()
      (
        SparkEnv.get.blockManager.blockManagerId,
        SparkEnv.get.executorId -> ctx.thread.getId,
        TaskContext.getPartitionId(),
        ctx
      )
  }
}

/**
  * Created by peng on 16/11/15.
  */
class SpookyViewsSuite extends SpookyEnvFixture {

  import SpookyViews._
  import org.scalatest.Matchers._
  import SpookyViewsSuite._

  it("multiPassFlatMap should yield same result as flatMap") {

    val src = sc.parallelize(1 to 100).persist()

    val counter = Metrics.accumulator(0)
    val counter2 = Metrics.accumulator(0)

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

  it("TraversableLike.filterByType should work on primitive types") {

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

  it("Array.filterByType should work on primitive types") {

    assert(Array(1, 2.2, "a").filterByType[Int].toSeq == Seq(1))
    assert(Array(1, 2.2, "a").filterByType[java.lang.Integer].toSeq == Seq(1: java.lang.Integer))
    assert(Array(1, 2.2, "a").filterByType[Double].toSeq == Seq(2.2))
    assert(Array(1, 2.2, "a").filterByType[java.lang.Double].toSeq == Seq(2.2: java.lang.Double))
    assert(Array(1, 2.2, "a").filterByType[String].toSeq == Seq("a"))
  }

  val nullStr = null: String
  it(":/ can handle null component") {

    assert(nullStr :/ nullStr :/ "abc" :/ null :/ null == "abc")
  }

  it("\\\\ can handle null component") {

    assert(nullStr \\ nullStr \\ "abc" \\ null \\ null == "abc")
  }


  import com.tribbloids.spookystuff.utils.SpookyViews._

  it("injectPassthroughPartitioner should not move partitions") {
    val rdd1: RDD[WithID] = sc.parallelize(1 to 100)
      .map(v => WithID(v))
    val rdd2: RDD[(Int, WithID)] = rdd1.injectPassthroughPartitioner
    assert(rdd2.partitioner.get.getClass == classOf[PartitionIdPassthrough])

    val zipped = rdd1.zipPartitions(rdd2) {
      (itr1, itr2) =>
        val a1 = itr1.toList.sortBy(_.hashCode)
        val a2 = itr2.map(_._2).toList.sortBy(_.hashCode)
        Iterator(
          a1 -> a2
        )
    }

    val array = zipped.collect()
    array.foreach(println)
    assert(array.count(v => v._1 == v._2) == array.length)
  }

  //TODO: doesn't work by now
  ignore("... even if the RDD is not Serializable") {

    val rdd1: RDD[WithID] = sc.parallelize(1 to 100)
      .map(v => NOTSerializableID(v))
    val rdd2: RDD[(Int, WithID)] = rdd1.injectPassthroughPartitioner
    assert(rdd2.partitioner.get.getClass == classOf[PartitionIdPassthrough])

    val zipped = rdd1.zipPartitions(rdd2) {
      (itr1, itr2) =>
        val a1 = itr1.toList.sortBy(_.hashCode)
        val a2 = itr2.map(_._2).toList.sortBy(_.hashCode)
        Iterator(
          a1 -> a2
        )
    }

    val array = zipped.collect()
    array.foreach(println)
    assert(array.count(v => v._1 == v._2) == array.length)
  }

  it("mapAtLeastOncePerExecutorCore will run properly") {
    val result = sc.mapAtLeastOncePerExecutorCore {
      getThreadInfo()
    }
      .collect()

    result.foreach(println)
    assert(result.length >= sc.defaultParallelism, result.mkString("\n"))
    assert(result.map(_._1).distinct.length == TestHelper.numWorkers, result.mkString("\n"))
    assert(result.map(_._2).distinct.length == result.length, result.mkString("\n"))
  }

  it("mapPerWorker will run properly") {
    val result = sc.mapPerWorker {
      getThreadInfo()
    }
      .collect()

    result.foreach(println)
    assert(result.length == TestHelper.numWorkers, result.mkString("\n"))
    assert(result.map(_._1).distinct.length == TestHelper.numWorkers, result.mkString("\n"))
    assert(result.map(_._2).distinct.length == TestHelper.numWorkers, result.mkString("\n"))
  }

  it("mapAtLeastOncePerCore will run properly") {
    val result = sc.mapAtLeastOncePerCore {
      getThreadInfo()
    }
      .collect()

    result.foreach(println)
    assert(result.length >= sc.defaultParallelism + 1, result.mkString("\n"))
    assert(result.map(_._1).distinct.length == TestHelper.numComputers, result.mkString("\n"))
    assert(result.map(_._2).distinct.length == result.length, result.mkString("\n"))
  }

  it("mapPerComputer will run properly") {
    val result = sc.mapPerComputer {
      getThreadInfo()
    }
      .collect()
    //+- 1 is for executor lost tolerance
    result.foreach(println)
    assert(result.length === TestHelper.numComputers +- 1, result.mkString("\n"))
    assert(result.map(_._1).distinct.length === TestHelper.numComputers +- 1, result.mkString("\n"))
    assert(result.map(_._2).distinct.length === TestHelper.numComputers +- 1, result.mkString("\n"))
  }

  it("result of allTaskLocationStrs can be used as partition's preferred location") {
    //TODO: change to more succinct ignore
    if (org.apache.spark.SPARK_VERSION.replaceAllLiterally(".","").toInt >= 160) {
      val tlStrs = sc.allTaskLocationStrs
      tlStrs.foreach(println)
      val length = tlStrs.size
      val seq: Seq[((Int, String), Seq[String])] = (1 to 100).map {
        i =>
          val nodeName = tlStrs(Random.nextInt(length))
          (i -> nodeName) -> Seq(nodeName)
      }

      val created = sc.makeRDD[(Int, String)](seq)
      //TODO: this RDD is extremely partitioned, can we use coalesce to reduce it?
      val conditions = created.map {
        tuple =>
          tuple._2 == SpookyUtils.getTaskLocationStr
      }
        .collect()
      assert(conditions.count(identity) == 100)
    }
  }

  it("interpolate can use common character as delimiter") {

    val original = "ORA'{TEST}"
    val interpolated = original.interpolate("'"){
      v =>
        "Replaced"
    }
    assert(interpolated == "ORAReplaced")
  }

  it("interpolate can use special regex character as delimiter") {

    val original = "ORA${TEST}"
    val interpolated = original.interpolate("$"){
      v =>
        "Replaced"
    }
    assert(interpolated == "ORAReplaced")
  }

  it("interpolate should ignore string that contains delimiter without bracket") {

    val original = "ORA$TEST"
    val interpolated = original.interpolate("$"){
      v =>
        "Replaced"
    }
    assert(interpolated == original)
  }

  it("interpolate should allow delimiter to be escaped") {

    val original = "ORA$${TEST}"
    val interpolated = original.interpolate("$"){
      v =>
        "Replaced"
    }
    assert(interpolated == original)
  }

  //  test("1") {
  //    println(Seq("abc", "def", 3, 4, 2.3).filterByType[String].get)
  //    println(Seq("abc", "def", 3, 4, 2.3).filterByType[Integer].get)
  //    println(Seq("abc", "def", 3, 4, 2.3).filterByType[Int].get)
  //    println(Seq("abc", "def", 3, 4, 2.3).filterByType[java.lang.Double].get)
  //    println(Seq("abc", "def", 3, 4, 2.3).filterByType[Double].get)
  //
  //    //    val res2: Array[String] = Array("abc", "def").filterByType[String].get
  //    //    println(res2)
  //
  //    println(Set("abc", "def", 3, 4, 2.3).filterByType[String].get)
  //    println(Set("abc", "def", 3, 4, 2.3).filterByType[Integer].get)
  //    println(Set("abc", "def", 3, 4, 2.3).filterByType[Int].get)
  //    println(Seq("abc", "def", 3, 4, 2.3).filterByType[java.lang.Double].get)
  //    println(Seq("abc", "def", 3, 4, 2.3).filterByType[Double].get)
  //  }
}
