package org.tribbloid.spookystuff

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.tribbloid.spookystuff.entity.{Key, KeyLike, PageRow}
import org.tribbloid.spookystuff.sparkbinding.{DataFrameView, PageRowRDD}
import org.tribbloid.spookystuff.utils.Utils

import scala.collection.immutable.{ListMap, ListSet}
import scala.language.implicitConversions
import scala.reflect.ClassTag

object Metrics {

  private def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T]) = {
    new Accumulator(initialValue, param, Some(name))
  }
}

case class Metrics(
                    driverInitialized: Accumulator[Int] = Metrics.accumulator(0, "driverInitialized"),
                    driverReclaimed: Accumulator[Int] = Metrics.accumulator(0, "driverReclaimed"),

                    sessionInitialized: Accumulator[Int] = Metrics.accumulator(0, "sessionInitialized"),
                    sessionReclaimed: Accumulator[Int] = Metrics.accumulator(0, "sessionReclaimed"),

                    DFSReadSuccess: Accumulator[Int] = Metrics.accumulator(0, "DFSReadSuccess"),
                    DFSReadFail: Accumulator[Int] = Metrics.accumulator(0, "DFSReadFail"),

                    DFSWriteSuccess: Accumulator[Int] = Metrics.accumulator(0, "DFSWriteSuccess"),
                    DFSWriteFail: Accumulator[Int] = Metrics.accumulator(0, "DFSWriteFail"),

                    pagesFetched: Accumulator[Int] = Metrics.accumulator(0, "pagesFetched"),
                    pagesFetchedFromWeb: Accumulator[Int] = Metrics.accumulator(0, "pagesFetchedFromWeb"),
                    pagesFetchedFromCache: Accumulator[Int] = Metrics.accumulator(0, "pagesFetchedFromCache")
                    ) {

  def toJSON: String = {
    val tuples = this.productIterator.flatMap{
      case acc: Accumulator[_] => acc.name.map(_ -> acc.value)
      case _ => None
    }.toSeq

    val map = ListMap(tuples: _*)

    Utils.toJson(map, beautiful = true)
  }
}

/*
  cannot be shipped to workers
  entry point of the pipeline
 */
case class SpookyContext (
                           @transient sqlContext: SQLContext, //can't be used on executors
                           @transient private val _spookyConf: SpookyConf = new SpookyConf(), //can only be used on executors after broadcast
                           var metrics: Metrics = new Metrics() //accumulators cannot be broadcasted,
                           //                           lookup: Lookup = Lookup()
                           // shared by multiple initialized PageRowRDDs. Make sure all attempts to change it are blocking! Should be fast as long as no action happens.
                           // this is disabled in favour of rdd cache in PageRowRDD
                           ) {

  //  if (lookup.rdd == null) {
  //    lookup.rdd = sqlContext.sparkContext.emptyRDD[(LookupKey, SquashedRow)]//.partitionBy(new HashPartitioner(conf.defaultParallelism))
  //  }

  def this(sqlContext: SQLContext) {
    this(sqlContext, new SpookyConf(), new Metrics())
  }

  def this(sc: SparkContext) {
    this(new SQLContext(sc))
  }

  def this(conf: SparkConf) {
    this(new SparkContext(conf))
  }

  @volatile var broadcastedSpookyConf = sqlContext.sparkContext.broadcast(_spookyConf)

  def conf = if (_spookyConf == null) broadcastedSpookyConf.value
  else _spookyConf

  def broadcast(): Unit ={
    broadcastedSpookyConf.destroy()
    broadcastedSpookyConf = sqlContext.sparkContext.broadcast(_spookyConf)
  }

  val broadcastedHadoopConf = if (sqlContext!=null) sqlContext.sparkContext.broadcast(new SerializableWritable(this.sqlContext.sparkContext.hadoopConfiguration))
  else null

  def hadoopConf: Configuration = broadcastedHadoopConf.value.value

  def zeroMetrics(): SpookyContext ={
    metrics = new Metrics()
    this
  }

  def getContextForNewInput = if (conf.sharedMetrics) this
  else this.copy(metrics = new Metrics())

  object dsl {

    implicit def DataFrameToPageRowRDD(df: DataFrame): PageRowRDD = {
      val self = new DataFrameView(df).toMapRDD.map {
        map =>
          PageRow(
            Option(ListMap(map.toSeq: _*))
              .getOrElse(ListMap())
              .map(tuple => (Key(tuple._1), tuple._2))
          )
      }
      new PageRowRDD(self, keys = ListSet(df.schema.fieldNames: _*).map(Key(_)), spooky = getContextForNewInput)
    }

    //every input or noInput will generate a new metrics
    implicit def RDDToPageRowRDD[T: ClassTag](rdd: RDD[T]): PageRowRDD = {
      import org.tribbloid.spookystuff.views._

      import scala.reflect._

      rdd match {
        case _ if classOf[Map[_,_]].isAssignableFrom(classTag[T].runtimeClass) => //use classOf everywhere?
          val canonRdd = rdd.map(
            map =>map.asInstanceOf[Map[_,_]].canonizeKeysToColumnNames
          )

          val jsonRDD = canonRdd.map(
            map =>
              Utils.toJson(map)
          )
          val dataFrame = sqlContext.jsonRDD(jsonRDD)
          val self = canonRdd.map(
            map =>
              PageRow(ListMap(map.map(tuple => (Key(tuple._1),tuple._2)).toSeq: _*), Array())
          )
          new PageRowRDD(self, keys = ListSet(dataFrame.schema.fieldNames: _*).map(Key(_)), spooky = getContextForNewInput)
        case _ =>
          val self = rdd.map{
            str =>
              var cells = ListMap[KeyLike,Any]()
              if (str!=null) cells = cells + (Key("_") -> str)

              PageRow(cells)
          }
          new PageRowRDD(self, keys = ListSet(Key("_")), spooky = getContextForNewInput)
      }
    }
  }
}