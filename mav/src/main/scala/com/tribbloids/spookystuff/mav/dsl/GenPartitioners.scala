package com.tribbloids.spookystuff.mav.dsl

import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.dsl.GenPartitioners.GenPartitionerImpl
import com.tribbloids.spookystuff.row.BeaconRDD
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by peng on 31/12/16.
  */
object GenPartitioners {

  object Voronoi extends GenPartitioner {

    override def getImpl: GenPartitionerImpl = GPImpl

    object GPImpl extends GenPartitionerImpl {

      override def groupByKey[K: ClassTag, V: ClassTag](
                                                         rdd: RDD[(K, V)],
                                                         beaconRDDOpt: Option[BeaconRDD[K]]
                                                       ): RDD[(K, Iterable[V])] = {
        ???
      }
    }
  }
}
