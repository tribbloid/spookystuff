package com.tribbloids.spookystuff.utils.locality

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.TestBeans._
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkException}
import org.scalatest.Assertions

import scala.reflect.ClassTag
import scala.util.Random

class LocalityImplSuite extends SpookyEnvFixture {

  import LocalityImplSuite._

  final val size = 100
  final val np = 9

  val baseArray = {
    val range = 1 to size
    val a1 = Random.shuffle(range)
    val a2 = Random.shuffle(range)
    a1.zip(a2)
  }

  val base = sc.parallelize(baseArray, np)

  def rdd1: RDD[(Int, WithID)] =
    base
      .map { v =>
        v._1 -> WithID(Random.nextInt(1000))
      }
      .persist()
  def rdd2: RDD[(Int, WithID)] =
    base
      .map { v =>
        v._2 -> WithID(Random.nextInt(1000))
      }
      .persist()

  describe("Spike: when 2 RDDs are cogrouped") {

    it("the first operand will NOT move if it has a partitioner") {
      val still = rdd1
        .partitionBy(new HashPartitioner(np))
      val moved = rdd2

      Validate(still, moved).assertLocalityWithoutOrder()
    }

    it(
      "the first operand containing unserializable objects will not trigger an exception" +
        "if it has a partitioner") {
      val still = rdd1
        .partitionBy(new HashPartitioner(np))
        .mapValues { v =>
          NOTSerializableID(v._id): WithID
        }
      val moved = rdd2

      if (!sc.isLocal) {
        intercept[SparkException] {
          still.collect()
        }
      }
      Validate(still, moved).assertLocalityWithoutOrder()
    }

    it("the second operand will NOT move if it has a partitioner") {
      val moved = rdd1
      val still = rdd2
        .partitionBy(new HashPartitioner(np))

      Validate(moved, still, firstStay = false).assertLocalityWithoutOrder()
    }

    it(
      "the second operand containing unserializable objects will not trigger an exception" +
        "if it has a partitioner") {
      val moved = rdd1
      val still = rdd2
        .partitionBy(new HashPartitioner(np))
        .mapValues { v =>
          NOTSerializableID(v._id): WithID
        }

      if (!sc.isLocal) {
        intercept[SparkException] {
          still.collect()
        }
      }
      Validate(moved, still, firstStay = false).assertLocalityWithoutOrder()
    }

    describe("in Spark 1.x") {
      if (spark.SPARK_VERSION.startsWith("1.")) {
        it("the second will NOT move if both have partitioners") {

          val still = rdd1
            .partitionBy(new HashPartitioner(np))
          val moved = rdd2
            .partitionBy(new RangePartitioner(np, rdd2))

          Validate(moved, still, firstStay = false).assertLocalityWithoutOrder()
        }

        it("the second will NOT move even if both have partitioners and the second is in memory") {

          val still = rdd1
            .partitionBy(new RangePartitioner(np, rdd1))
          val moved = rdd2
            .partitionBy(new HashPartitioner(np))
            .persist()
          moved.count()

          Validate(moved, still, firstStay = false).assertLocalityWithoutOrder()
        }
      }
    }

    describe("in Spark 2.x") {
      if (spark.SPARK_VERSION.startsWith("2.")) {
        it("the first will NOT move if both have partitioners") {

          val still = rdd1
            .partitionBy(new HashPartitioner(np))
          val moved = rdd2
            .partitionBy(new RangePartitioner(np, rdd2))

          Validate(moved, still).assertLocalityWithoutOrder()
        }

        it("the first will NOT move even if both have partitioners and the second is in memory") {

          val still = rdd1
            .partitionBy(new RangePartitioner(np, rdd1))
          val moved = rdd2
            .partitionBy(new HashPartitioner(np))
            .persist()
          moved.count()

          Validate(moved, still).assertLocalityWithoutOrder()
        }
      }
    }
  }

  //TODO: duplicate, delete!
  ignore("each partition of the first operand of cogroup should not move, but elements are shuffled") {
    // sc is the SparkContext
    val rdd1: RDD[(Int, Int)] = sc
      .parallelize(1 to 10, 4)
      .map(v => v -> v)
      .partitionBy(new HashPartitioner(4))
    rdd1.persist().count()

    val rdd2: RDD[(Int, Int)] = sc
      .parallelize(1 to 10, 4)
      .map(v => (11 - v) -> v)

    val cogrouped: RDD[(Int, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    val joined: RDD[(Int, (Int, Int))] = cogrouped.mapValues { v =>
      v._1.head -> v._2.head
    }

    println(joined.toDebugString)

    val zipped = joined.zipPartitions(rdd1, rdd2) { (itr1, itr2, itr3) =>
      val list1 = itr1.toList
      val a1 = list1.map(_._2._1)
      val b1 = list1.map(_._2._2)
      val a2 = itr2.map(_._2).toList
      val b2 = itr3.map(_._2).toList

      Iterator(
        (a1, b1, a2, b2)
      )
    }

    val array = zipped.collect()
    println(array.mkString("\n"))
    assert(array.count(v => v._1.sorted == v._3.sorted) == array.length)
    assert(array.count(v => v._2.sorted == v._4.sorted) < array.length)
    assert(array.count(v => v._1 == v._3) < array.length)
  }

  describe("cogroupBase() can preserve both locality and in-partition orders") {

    val allImpls: Seq[Locality_OrdinalityImpl[Int, WithID]] = {
      def partitioned =
        rdd1
          .partitionBy(new HashPartitioner(np))

      Seq(
        BroadcastLocalityImpl(rdd1),
        IndexingLocalityImpl(rdd1),
        SortingLocalityImpl(partitioned)
      )
    }
    testAllImpls(allImpls)
  }

  private def testAllImpls(
      allImpls: Seq[Locality_OrdinalityImpl[Int, WithID]],
      rdd2: RDD[(Int, WithID)] = this.rdd2
  ) = {
    allImpls.foreach { impl =>
      it(impl.getClass.getSimpleName) {
        val first = impl.rdd1
        val second = rdd2
        val result = impl.cogroupBase(second)
        Validate(
          first,
          second,
          cogroupBaseOverride = Some(result)
        ).assertLocalityAndOrder()
      }
    }
  }

  describe("... even if the first operand is not serializable") {

    val allImpls: Seq[Locality_OrdinalityImpl[Int, WithID]] = {
      val rdd = rdd1.mapValues { v =>
        NOTSerializableID(v._id): WithID
      }

      def partitioned = rdd1.partitionBy(new HashPartitioner(np)).mapValues { v =>
        NOTSerializableID(v._id): WithID
      }

      Seq(
        BroadcastLocalityImpl(rdd),
        IndexingLocalityImpl(rdd),
        SortingLocalityImpl(partitioned)
      )
    }
    testAllImpls(allImpls)
  }

  describe("... even if keys overlap partially") {
    val keys1 = (1 to 10).map(_ + size)
    val keys2 = (11 to 20).map(_ + size)

    val rdd1_extra = rdd1.union(
      sc.parallelize(keys1).map { i =>
        i -> WithID(i)
      }
    )
    val rdd2_extra = rdd2.union(
      sc.parallelize(keys2).map { i =>
        i -> WithID(i)
      }
    )
    val allImpls: Seq[Locality_OrdinalityImpl[Int, WithID]] = {
      def partitioned =
        rdd1_extra
          .partitionBy(new HashPartitioner(rdd1_extra.partitions.length))

      Seq(
        BroadcastLocalityImpl(rdd1_extra),
        IndexingLocalityImpl(rdd1_extra),
        SortingLocalityImpl(partitioned)
      )
    }
    testAllImpls(allImpls, rdd2_extra)
    allImpls.foreach { impl =>
      it(impl.getClass.getSimpleName + ".cogroupBase() is always left-outer") {

        val result = impl.cogroupBase(rdd2_extra)
        val keys = result.keys.persist()
        val count1 = keys.filter(i => keys1.contains(i)).count()
        val count2 = keys.filter(i => keys2.contains(i)).count()

        assert(count1 == 10)
        assert(count2 == 0)
      }
    }
  }
}

object LocalityImplSuite extends Assertions {

  case class Validate[K: ClassTag, V: ClassTag](
      first: RDD[(K, V)],
      second: RDD[(K, V)],
      firstStay: Boolean = true,
      cogroupBaseOverride: Option[RDD[(K, (V, Iterable[V]))]] = None
  ) {

    assert(first.partitions.length == second.partitions.length, "number of partitions mismatch")

    val (shouldStay: (Int, Int), shouldMove: (Int, Int)) = if (firstStay) {
      (0 -> 2, 1 -> 3)
    } else {
      (1 -> 3, 0 -> 2)
    }

    val cogroupBase: RDD[(K, (V, Iterable[V]))] = cogroupBaseOverride.getOrElse {

      val cogrouped = first.cogroup[V](second)

      cogrouped.mapValues { triplet =>
        Predef.assert(triplet._1.size == 1)
        triplet._1.head -> triplet._2
      }
    }

    val cogroupedValues: RDD[(V, Iterable[V])] = cogroupBase.values

    val allZipped: RDD[List[List[V]]] = cogroupedValues
      .zipPartitions(first, second) { (itr1, itr2, itr3) =>
        val first = itr2.map(_._2).toList
        val second = itr3.map(_._2).toList
        val cogrouped = itr1.toList

        Iterator(List(first, second, cogrouped.map(_._1), cogrouped.flatMap(_._2)))
      }
      .persist()
    first.persist()
    second.persist()

    val allZipped_sorted = allZipped
      .map { v =>
        v.map(vv => vv.sortBy(_.hashCode))
      }
      .persist()

    val size = allZipped.count()

    val array = allZipped_sorted.map(v => "" + v).collect()
    array.foreach(println)

    def assertLocality(): Unit = {

      assert(allZipped_sorted.filter(v => v(shouldStay._1) == v(shouldStay._2)).count() == size)
      assert(allZipped_sorted.filter(v => v(shouldMove._1) == v(shouldMove._2)).count() < size)
    }

    def assertLocalityWithoutOrder(): Unit = {
      assertLocality()
      assert(allZipped.filter(v => v(shouldStay._1) == v(shouldStay._2)).count() < size)
    }

    def assertLocalityAndOrder(): Unit = {
      assertLocality()
      assert(allZipped.filter(v => v(shouldStay._1) == v(shouldStay._2)).count() == size)
    }
  }
}
