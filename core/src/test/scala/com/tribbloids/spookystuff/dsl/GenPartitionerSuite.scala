package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.SpookyEnvFixture
import org.apache.spark.HashPartitioner

import scala.util.Random

/**
  * Created by peng on 20/12/16.
  */
class GenPartitionerSuite extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  test("DocCacheAware can co-partition 2 RDDs") {
    val numPartitions = Random.nextInt(80) + 9

    val gp = GenPartitioners.DocCacheAware(_ => new HashPartitioner(numPartitions)).getImpl(spooky)
    val beaconOpt = gp.createBeaconRDD[Int](sc.emptyRDD[Int])
    //    val beacon = sc.makeRDD(1 to 1000, 1000).map(v => v -> v*v)

    //TODO: this will have size 1 in local mode which will seriously screw up the following logic
    val tlStrs = sc.allTaskLocationStrs
    val size = tlStrs.length

    val src1 = (1 to 1000).map {
      v =>
        (v -> v.toString) -> Seq(tlStrs(Random.nextInt(size)))
    }
    val ref1 = sc.makeRDD[(Int, String)](src1)
      .coalesce(numPartitions + 5, shuffle = false)
      .persist()
    ref1.count()
    val src2 = (1 to 1000).map {
      v =>
        (v -> v.toString) -> Seq(tlStrs(Random.nextInt(size)))
    }
    val ref2 = sc.makeRDD[(Int, String)](src2)
      .coalesce(numPartitions + 5, shuffle = false)
      .persist()
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
