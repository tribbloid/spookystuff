package org.tribbloid.spookystuff

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SchemaRDD}
import org.tribbloid.spookystuff.entity.{Key, KeyLike, PageRow}
import org.tribbloid.spookystuff.sparkbinding.{PageRowRDD, SchemaRDDView, StringRDDView}
import org.tribbloid.spookystuff.utils.Utils

import scala.collection.immutable.ListSet
import scala.language.implicitConversions
import scala.reflect.ClassTag

class Metrics() extends Serializable {

  //works but temporarily disabled as not part of 'official' API
  private def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T]) = {
    new Accumulator(initialValue, param, Some(name))
  }

  import org.apache.spark.SparkContext._

  val driverInitialized: Accumulator[Int] = accumulator(0, "driverInitialized")
  val driverReclaimed: Accumulator[Int] = accumulator(0, "driverReclaimed")

  val sessionInitialized: Accumulator[Int] = accumulator(0, "sessionInitialized")
  val sessionReclaimed: Accumulator[Int] = accumulator(0, "sessionReclaimed")

  val DFSReadSuccess: Accumulator[Int] = accumulator(0, "DFSReadSuccess")
  val DFSReadFail: Accumulator[Int] = accumulator(0, "DFSReadFail")

  val DFSWriteSuccess: Accumulator[Int] = accumulator(0, "DFSWriteSuccess")
  val DFSWriteFail: Accumulator[Int] = accumulator(0, "DFSWriteFail")

  val pagesFetched: Accumulator[Int] = accumulator(0, "pagesFetched")
  val pagesFetchedFromWeb: Accumulator[Int] = accumulator(0, "pagesFetchedFromWeb")
  val pagesFetchedFromCache: Accumulator[Int] = accumulator(0, "pagesFetchedFromCache")
}

class SpookyContext (
                      @transient val sqlContext: SQLContext, //can't be used on executors
                      @transient private val _spookyConf: SpookyConf = new SpookyConf(), //can only be used on executors after broadcast
                      var metrics: Metrics = new Metrics() //don't broadcast
                      )
  extends Serializable {

  def this(sqlContext: SQLContext) {
    this(sqlContext, new SpookyConf(), new Metrics())
  }

  def this(sc: SparkContext) {
    this(new SQLContext(sc))
  }

  def this(conf: SparkConf) {
    this(new SparkContext(conf))
  }

  import org.tribbloid.spookystuff.views._
  
  var broadcastedSpookyConf = sqlContext.sparkContext.broadcast(_spookyConf)

  def conf = if (_spookyConf == null) broadcastedSpookyConf.value
  else _spookyConf

  def broadcast(): SpookyContext ={
    var broadcastedSpookyConf = sqlContext.sparkContext.broadcast(_spookyConf)
    this
  }

  val broadcastedHadoopConf = if (sqlContext!=null) sqlContext.sparkContext.broadcast(new SerializableWritable(this.sqlContext.sparkContext.hadoopConfiguration))
  else null

  def hadoopConf: Configuration = broadcastedHadoopConf.value.value

  def clearMetrics(): SpookyContext ={
    metrics = new Metrics()
    this
  }

  @transient lazy val noInput: PageRowRDD = PageRowRDD(this.sqlContext.sparkContext.noInput, spooky = this)

  implicit def stringRDDToItsView(rdd: RDD[String]): StringRDDView = new StringRDDView(rdd)

  implicit def schemaRDDToItsView(rdd: SchemaRDD): SchemaRDDView = new SchemaRDDView(rdd)

  //  implicit def selfToPageRowRDD(rdd: RDD[PageRow]): PageRowRDD = PageRowRDD(rdd, spooky = this)

  implicit def RDDToPageRowRDD[T: ClassTag](rdd: RDD[T]): PageRowRDD = {
    import org.tribbloid.spookystuff.views._

    import scala.reflect._

    rdd match {
      case rdd: SchemaRDD =>
        val self = new SchemaRDDView(rdd).asMapRDD.map{
          map =>
            new PageRow(
              Option(map)
                .getOrElse(Map())
                .map(tuple => (Key(tuple._1),tuple._2))
            )
        }
        new PageRowRDD(self, keys = ListSet(rdd.schema.fieldNames: _*).map(Key(_)), spooky = this)
      case _ if classOf[Map[_,_]].isAssignableFrom(classTag[T].runtimeClass) => //use classOf everywhere?
        val canonRdd = rdd.map(
          map =>map.asInstanceOf[Map[_,_]].canonizeKeysToColumnNames
        )

        val jsonRDD = canonRdd.map(
          map =>
            Utils.toJson(map)
        )
        val schemaRDD = sqlContext.jsonRDD(jsonRDD)
        val self = canonRdd.map(
          map =>
            PageRow(map.map(tuple => (Key(tuple._1),tuple._2)), Seq())
        )
        new PageRowRDD(self, keys = ListSet(schemaRDD.schema.fieldNames: _*).map(Key(_)), spooky = this)
      case _ =>
        val self = rdd.map{
          str =>
            var cells = Map[KeyLike,Any]()
            if (str!=null) cells = cells + (Key("_") -> str)

            new PageRow(cells)
        }
        new PageRowRDD(self, keys = ListSet(Key("_")), spooky = this)
    }
  }
}