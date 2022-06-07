package com.tribbloids.spookystuff.spike

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.Ignore

//TODO: delete after https://stackoverflow.com/questions/57482120/in-apache-spark-how-to-convert-a-slow-rdd-dataset-into-a-stream
@Ignore
class SlowRDDSpike extends FunSpecx {

  lazy val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  lazy val sc: SparkContext = spark.sparkContext
  lazy val sqlContext: SQLContext = spark.sqlContext

  import sqlContext.implicits._

  describe("is repartitioning non-blocking?") {

    it("dataset") {

      val ds = sqlContext
        .createDataset(1 to 100)
        .repartition(1)
        .mapPartitions { itr =>
          itr.map { ii =>
            Thread.sleep(100)
            println(f"skewed - $ii")
            ii
          }
        }

      val mapped = ds

      val mapped2 = mapped
        .repartition(10)
        .map { ii =>
          Thread.sleep(400)
          println(f"repartitioned - $ii")
          ii
        }

      mapped2.foreach { _ => }

//      Thread.sleep(1000000)
    }
  }

  it("RDD") {
    val ds = sc
      .parallelize(1 to 100)
      .repartition(1)
      .mapPartitions { itr =>
        itr.map { ii =>
          Thread.sleep(100)
          println(f"skewed - $ii")
          ii
        }
      }

    val mapped = ds

    val mapped2 = mapped
      .repartition(10)
      .map { ii =>
        Thread.sleep(400)
        println(f"repartitioned - $ii")
        ii
      }

    mapped2.foreach { _ => }

//    Thread.sleep(1000000)

  }

}
