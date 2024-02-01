package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.function.Impl
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * Created by peng on 20/12/16.
  */
class GenPartitionerSuite extends SpookyBaseSpec {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  it("DocCacheAware can co-partition 2 RDDs") {
    val numPartitions = Random.nextInt(80) + 9

    val gp = GenPartitioners
      .DocCacheAware(Impl { _ =>
        new HashPartitioner(numPartitions)
      })
      .getInstance[Int](defaultSchema)
    val beaconOpt = gp.createBeaconRDD(sc.emptyRDD[Int])
    //    val beacon = sc.makeRDD(1 to 1000, 1000).map(v => v -> v*v)

    //    val tlStrs = sc.allExecutorCoreLocationStrs
    //    val size = tlStrs.length

    val srcRDD: RDD[(Int, String)] = sc
      .parallelize(
        {
          (1 to 1000).map { v =>
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

    val zipped1 = ref1
      .map(_._2)
      .zipPartitions(ref2.map(_._2))((i1, i2) => Iterator(i1.toSet == i2.toSet))
      .collect()

    assert(zipped1.length > zipped1.count(identity))
    assert(zipped1.count(identity) < 2)

    val grouped1 = gp.groupByKey(ref1, beaconOpt).flatMap(_._2)
    val grouped2 = gp.groupByKey(ref2, beaconOpt).flatMap(_._2)

    val zipped2RDD = grouped1.zipPartitions(grouped2)((i1, i2) => Iterator(i1.toSet == i2.toSet))
    val zipped2 = zipped2RDD.collect()
    assert(zipped2.length == zipped2.count(identity))
    assert(zipped2RDD.partitions.length == numPartitions)
  }
}
