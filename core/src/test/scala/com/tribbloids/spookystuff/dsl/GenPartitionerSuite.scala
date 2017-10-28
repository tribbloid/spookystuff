package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.utils.{NOTSerializable, PartitionIdPassthrough}
import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.rdd.RDD

import scala.util.Random

object GenPartitionerSuite {

  case class WithID(id: Long)

  def NOTSerializableID(id: Long) = new WithID(id) with NOTSerializable
}

/**
  * Created by peng on 20/12/16.
  */
class GenPartitionerSuite extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  //TODO: doesn't work in 1.6.x, sould find new ways to control fine tune location in 2.1.x
  //  describe("Spike: when 2 RDDs are cogrouped"){
  //
  //    val sc = this.sc
  //    val base = sc.parallelize((1 to 10).zip(10.to(1, -1)))
  //      .map(v => v -> v._1)
  //    base.persist().collect()
  //    def pair1 = base.map { v => v._1._1 -> WithID(v._2)}
  //    def pair2 = base.map { v => v._1._2 -> WithID(v._2)}
  //
  //    val shouldMove = pair1
  //
  //    def assertNotMoved(
  //                        still: RDD[(Int, WithID)],
  //                        moved: RDD[(Int, WithID)]
  //                      ) = {
  //
  //      //      still.persist()
  //      //      moved.persist()
  //
  //      val cogrouped = still.cogroup(moved)
  //
  //      val idStill_idMoved = cogrouped.map {
  //        triplet =>
  //          Predef.assert(triplet._2._1.size == 1)
  //          Predef.assert(triplet._2._2.size == 1)
  //          triplet._2._1.head.id -> triplet._2._2.last.id
  //      }
  //        .persist()
  //
  //      val withStill = idStill_idMoved.zipPartitions(still)(
  //        (itr1, itr2) =>
  //          itr1.zipAll(
  //            itr2.map(_._2.id), 0->0 ,0
  //          )
  //      )
  //        .collect()
  //
  //      val withMoved = idStill_idMoved.zipPartitions(moved)(
  //        (itr1, itr2) =>
  //          itr1.zipAll(
  //            itr2.map(_._2.id), 0->0 ,0
  //          )
  //      )
  //        .collect()
  //
  //      val still_moved = still.zipPartitions(moved) {
  //        (itr1, itr2) =>
  //          itr1.map(_._2.id).zipAll(
  //            itr2.map(_._2.id), 0 ,0
  //          )
  //      }
  //        .collect()
  //
  //      assert(withStill.count(v => v._1._1 == v._2) == withStill.length)
  //      assert(withMoved.count(v => v._1._2 == v._2) < withMoved.length)
  //
  //      //      assert {
  //      //        val count = thread_idStill_idMoved.count(v => v._2._1 == v._2._2)
  //      //        count < thread_idStill_idMoved.length
  //      //      }
  //    }
  //
  //    it("the one with partitioner will NOT move") {
  //      val np = pair2.partitions.length
  //      val shouldStill = pair2
  //      // .partitionBy(new HashPartitioner(np))
  //      shouldStill.persist().count()
  //
  //      assertNotMoved(shouldStill, shouldMove)
  //    }
  //
  //    it("the first will NOT move") {
  //      val shouldStill = pair2
  //      shouldStill.persist().count()
  //
  //      assertNotMoved(shouldStill, shouldMove)
  //    }
  //
  //    it("the first will NOT move even if the second has a partitioner") {
  //
  //      val shouldMove = pair1
  //        .partitionBy(new HashPartitioner(10))
  //      assert(shouldMove.partitioner.nonEmpty)
  //
  //      val shouldStill = pair2
  //      shouldStill.persist().count()
  //
  //      assertNotMoved(shouldStill, shouldMove)
  //    }
  //
  //    it("the first will NOT move even if the second is persisted and has a partitioner") {
  //
  //      val shouldMove = pair1
  //        .partitionBy(new HashPartitioner(10))
  //      assert(shouldMove.partitioner.nonEmpty)
  //
  //      val shouldStill = pair2
  //      shouldStill.persist().count()
  //
  //      assertNotMoved(shouldStill, shouldMove)
  //    }
  //
  //    it("the one persisted but contain unserializable objects will still trigger an exception") {
  //      val shouldStill = pair2.mapValues {
  //        v =>
  //          NOTSerializableID(v.id): WithID
  //      }
  //      shouldStill.persist().count()
  //      intercept[SparkException] {
  //        assertNotMoved(shouldStill, shouldMove)
  //      }
  //    }
  //  }

  it("each partition of the first operand of cogroup should not move, but elements are shuffled") {
    // sc is the SparkContext
    val rdd1: RDD[(Int, Int)] = sc.parallelize(1 to 10, 4)
      .map(v => v->v)
      .partitionBy(new HashPartitioner(4))
    rdd1.persist().count()

    val rdd2: RDD[(Int, Int)] = sc.parallelize(1 to 10, 4)
      .map(v => (11-v)->v)

    val cogrouped: RDD[(Int, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    val joined: RDD[(Int, (Int, Int))] = cogrouped.mapValues {
      v =>
        v._1.head -> v._2.head
    }

    println(joined.toDebugString)

    val zipped = joined.zipPartitions(rdd1, rdd2) {
      (itr1, itr2, itr3) =>
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
    array.foreach(println)
    assert(array.count(v => v._1.sorted == v._3.sorted) == array.length)
    assert(array.count(v => v._2.sorted == v._4.sorted) < array.length)
    assert(array.count(v => v._1 == v._3) < array.length)
  }

  it("repartition with PartitionIDPassthrough should not move partitions") {
    val rdd1: RDD[(Int, Int)] = sc.parallelize(Seq.tabulate(10)(i => Random.nextInt(100)), 4)
      .map(v => TaskContext.get().partitionId() -> v)

    val rdd2 = rdd1.partitionBy(new PartitionIdPassthrough(4))
    assert(rdd2.partitioner.get.getClass == classOf[PartitionIdPassthrough])

    val zipped = rdd1.zipPartitions(rdd2) {
      (itr1, itr2) =>
        val a1 = itr1.toList.sorted
        val a2 = itr2.toList.sorted
        Iterator(
          a1 -> a2
        )
    }

    val array = zipped.collect()
    array.foreach(println)
    assert(array.count(v => v._1 == v._2) == array.length)
  }

  it("DocCacheAware can co-partition 2 RDDs") {
    val numPartitions = Random.nextInt(80) + 9

    val gp = GenPartitioners.DocCacheAware(_ => new HashPartitioner(numPartitions))
      .getInstance[Int](defaultSchema)
    val beaconOpt = gp.createBeaconRDD(sc.emptyRDD[Int])
    //    val beacon = sc.makeRDD(1 to 1000, 1000).map(v => v -> v*v)

    //    val tlStrs = sc.allExecutorCoreLocationStrs
    //    val size = tlStrs.length

    val srcRDD: RDD[(Int, String)] = sc.parallelize (
      {
        (1 to 1000).map {
          v =>
            v -> v.toString
        }
      },
      numPartitions + 5
    )
      .persist()

    val ref1 = srcRDD.shufflePartitions.persist()
    ref1.count()

    val ref2 = srcRDD.shufflePartitions.persist()
    ref2.count()

    //    ref1.mapPartitions(i => Iterator(i.toList)).collect().foreach(println)
    //    ref2.mapPartitions(i => Iterator(i.toList)).collect().foreach(println)

    val zipped1 = ref1.map(_._2).zipPartitions(ref2.map(_._2))(
      (i1, i2) =>
        Iterator(i1.toSet == i2.toSet)
    )
      .collect()

    assert(zipped1.length > zipped1.count(identity))
    assert(zipped1.count(identity) < 2)

    val grouped1 = gp.groupByKey(ref1, beaconOpt).flatMap(_._2)
    val grouped2 = gp.groupByKey(ref2, beaconOpt).flatMap(_._2)

    val zipped2RDD = grouped1.zipPartitions(grouped2)(
      (i1, i2) =>
        Iterator(i1.toSet == i2.toSet)
    )
    val zipped2 = zipped2RDD.collect()
    assert(zipped2.length == zipped2.count(identity))
    assert(zipped2RDD.partitions.length == numPartitions)
  }
}
