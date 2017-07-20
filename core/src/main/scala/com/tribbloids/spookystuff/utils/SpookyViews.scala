package com.tribbloids.spookystuff.utils

import java.security.PrivilegedAction

import com.tribbloids.spookystuff.caching.ConcurrentMap
import com.tribbloids.spookystuff.row._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext, TaskContext}

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.ListMap
import scala.collection.{Map, TraversableLike}
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by peng on 11/7/14.
  * implicit conversions in this package are used for development only
  */
case object SpookyViews {

  val SPARK_JOB_DESCRIPTION = "spark.job.description"
  val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"
  val RDD_SCOPE_KEY = "spark.rdd.scope"
  val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"

  implicit class Function2PrivilegedAction[T](f: => T) extends PrivilegedAction[T] {
    override def run(): T = {
      f
    }
  }

  // (stageID -> threadID) -> isExecuted
  val perCoreMark: ConcurrentMap[(Int, Long), Boolean] = ConcurrentMap()
  // stageID -> isExecuted
  val perWorkerMark: ConcurrentMap[Int, Boolean] = ConcurrentMap()

  implicit class SparkContextView(val self: SparkContext) {

    def withJob[T](description: String)(fn: T): T = {

      val oldDescription = self.getLocalProperty(SPARK_JOB_DESCRIPTION)
      if (oldDescription == null) self.setJobDescription(description)
      else self.setJobDescription(oldDescription + " > " + description)

      val result: T = fn
      self.setJobGroup(null,oldDescription)
      result
    }

    // large enough such that all idle threads has a chance to pick up >1 partition
    val SEED_REPLICATING_FACTOR = 16
    /**
      * guaranteed to have at least 1 datum on each executor thread. Better distribute evenly
      */
    private def seed(
                      sizeOpt: Option[Int]
                    ) = {
      val size = sizeOpt.getOrElse(self.defaultParallelism * SEED_REPLICATING_FACTOR)
      val seed = self.makeRDD(1 to size, size)
        .map(i => i->i)
        .sortByKey(numPartitions = size)
      //        .partitionBy(new HashPartitioner(self.defaultParallelism)) //TODO: should use RangePartitioner?
      //        .persist()
      //      seed.count()
      //      val seed = self.makeRDD[Int]((1 to self.defaultParallelism).map(i => i -> Seq(i.toString)))
      assert(
        seed.partitions.length == size,
        s"seed doesn't have the right number of partitions: expected $size, actual ${seed.partitions.length}"
      )
      seed
    }

    def mapAtLeastOncePerExecutorCore[T: ClassTag](
                                                    f: => T,
                                                    sizeOpt: Option[Int] = None
                                                  ): RDD[T] = {

      seed(sizeOpt).mapPartitions {
        itr =>
          val stageID = TaskContext.get.stageId()
          //          val executorID = SparkEnv.get.executorId //technically this is useless as the map is only shared locally but whatever
          val threadID = Thread.currentThread().getId
          val allIDs = stageID -> threadID
          val alreadyRun = perCoreMark.synchronized {
            val alreadyRun = perCoreMark.getOrElseUpdate(allIDs, false)
            if (!alreadyRun) {
              perCoreMark.put(allIDs, true)
            }
            alreadyRun
          }
          if (!alreadyRun) {
            //            Thread.sleep(1000)
            Iterator(f)
          }
          else {
            Iterator.empty
          }
      }
    }
    def exeAtLeastOncePerExecutorCore[T: ClassTag](
                                                    f: => T,
                                                    sizeOpt: Option[Int] = None
                                                  ) =
      mapAtLeastOncePerExecutorCore(f, sizeOpt).count()

    //TODO: change to concurrent execution
    def mapAtLeastOncePerCore[T: ClassTag](
                                            f: => T,
                                            sizeOpt: Option[Int] = None
                                          ): RDD[T] = {
      val perExec = mapAtLeastOncePerExecutorCore(f, sizeOpt)
      val v = f
      self.makeRDD(Seq(v), 1).union(perExec)
    }

    def exeAtLeastOncePerCore[T: ClassTag](
                                            f: => T,
                                            sizeOpt: Option[Int] = None
                                          ): Long = {
      mapAtLeastOncePerCore(f, sizeOpt).count()
    }

    def mapPerWorker[T: ClassTag](
                                   f: => T,
                                   sizeOpt: Option[Int] = None
                                 ): RDD[T] = {

      seed(sizeOpt).mapPartitions {
        itr =>
          val stageID = TaskContext.get.stageId()
          val alreadyRun = perWorkerMark.synchronized {
            val alreadyRun = perWorkerMark.getOrElseUpdate(stageID, false)
            if (!alreadyRun) {
              perWorkerMark.put(stageID, true)
            }
            alreadyRun
          }
          if (!alreadyRun) {
            Thread.sleep(1000)
            Iterator(f)
          }
          else {
            Iterator.empty
          }
      }
    }
    def foreachWorker[T: ClassTag](
                                    f: => T,
                                    sizeOpt: Option[Int] = None
                                  ): Long = mapPerWorker(f, sizeOpt).count()

    //TODO: change to concurrent execution
    def mapPerComputer[T: ClassTag](
                                     f: => T,
                                     sizeOpt: Option[Int] = None
                                   ): RDD[T] = {
      val v = f

      if (self.isLocal) {
        self.makeRDD(Seq(v), 1)
      }
      else {
        val perWorker = mapPerWorker(f, sizeOpt)
        self.makeRDD(Seq(v)).union(perWorker)
      }
    }
    def foreachComputer[T: ClassTag](
                                      f: => T,
                                      sizeOpt: Option[Int] = None
                                    ): Long = {
      mapPerComputer(f, sizeOpt).count()
    }

    def allTaskLocationStrs: Seq[String] = {
      mapPerWorker {
        SpookyUtils.getTaskLocationStr
      }
        .collect()
    }

    //TODO: remove! not useful
    def allExecutorCoreIDs = {
      mapAtLeastOncePerExecutorCore {
        val thread = Thread.currentThread()
        (SpookyUtils.getBlockManagerID, thread.getId, thread.getName)
      }
        .collect()
    }
  }

  implicit class RDDView[T](val self: RDD[T]) {

    def collectPerPartition: Array[List[T]] = self.mapPartitions(
      v =>
        Iterator(v.toList)
    )
      .collect()

    def multiPassMap[U: ClassTag](f: T => Option[U]): RDD[U] = {

      multiPassFlatMap(f.andThen(v => v.map(Traversable(_))))
    }

    //if the function returns None for it will be retried as many times as it takes to get rid of them.
    //core problem is optimization: how to SPILL properly and efficiently?
    //TODO: this is the first implementation, simple but may not the most efficient
    def multiPassFlatMap[U: ClassTag](f: T => Option[TraversableOnce[U]]): RDD[U] = {

      val counter = self.sparkContext.accumulator(0, "unprocessed data")
      var halfDone: RDD[Either[T, TraversableOnce[U]]] = self.map(v => Left(v))

      while(true) {
        counter.setValue(0)

        val updated: RDD[Either[T, TraversableOnce[U]]] = halfDone.map {
          case Left(src) =>
            f(src) match {
              case Some(res) => Right(res)
              case None =>
                counter += 1
                Left(src)
            }
          case Right(res) => Right(res)
        }

        updated.persist().count()
        halfDone.unpersist()

        if (counter.value == 0) return updated.flatMap(_.right.get)

        halfDone = updated
      }
      sys.error("impossible")

      //      self.mapPartitions{
      //        itr =>
      //          var intermediateResult: Iterator[Either[T, TraversableOnce[U]]] = itr.map(v => Left(v))
      //
      //          var unfinished = true
      //          while (unfinished) {
      //
      //            var counter = 0
      //            val updated: Iterator[Either[T, TraversableOnce[U]]] = intermediateResult.map {
      //              case Left(src) =>
      //                f(src) match {
      //                  case Some(res) => Right(res)
      //                  case None =>
      //                    counter = counter + 1
      //                    Left(src)
      //                }
      //              case Right(res) => Right(res)
      //            }
      //            intermediateResult = updated
      //
      //            if (counter == 0) unfinished = false
      //          }
      //
      //          intermediateResult.flatMap(_.right.get)
      //      }
    }

    //    def persistDuring[T](newLevel: StorageLevel, blocking: Boolean = true)(fn: => T): T =
    //      if (self.getStorageLevel == StorageLevel.NONE){
    //        self.persist(newLevel)
    //        val result = fn
    //        self.unpersist(blocking)
    //        result
    //      }
    //      else {
    //        val result = fn
    //        self.unpersist(blocking)
    //        result
    //      }

    //  def checkpointNow(): Unit = { TODO: is it useless now?
    //    persistDuring(StorageLevel.MEMORY_ONLY) {
    //      self.checkpoint()
    //      self.foreach(_ =>)
    //      self
    //    }
    //    Unit
    //  }

    def isPersisted: Boolean = {
      val rddInfos = self.sparkContext.getRDDStorageInfo
      rddInfos.find(_.id == self.id).get.storageLevel != StorageLevel.NONE
    }

    def assertIsBeacon(): Unit = {
      assert(isPersisted)
      assert(self.isEmpty())
    }

    def shufflePartitions(implicit ev: ClassTag[T]): RDD[T] = {

      val randomKeyed: RDD[(Long, T)] = self.keyBy(_ => Random.nextLong())
      val shuffled = randomKeyed.partitionBy(new HashPartitioner(self.partitions.length))
      shuffled.values
    }
  }

  implicit class StringRDDView(val self: RDD[String]) {

    //csv has to be headerless, there is no better solution as header will be shuffled to nowhere
    def csvToMap(headerRow: String, splitter: String = ","): RDD[Map[String,String]] = {
      val headers = headerRow.split(splitter)

      //cannot handle when a row is identical to headerline, but whatever
      self.map {
        str => {
          val values = str.split(splitter)

          ListMap(headers.zip(values): _*)
        }
      }
    }

    def tsvToMap(headerRow: String) = csvToMap(headerRow,"\t")
  }

  implicit class PairRDDView[K: ClassTag, V: ClassTag](val self: RDD[(K, V)]) {

    import RDD._
    //get 3 RDDs that shares key partitioning: leftExclusive, intersection, rightExclusive
    //all 3 can be zipped directly as if joined by key, this has many applications like getting union, intersection and subtraction
    //    def logicalCombinationsByKey[S](
    //                                     other: RDD[(K, V)])(
    //                                     innerReducer: (V, V) => V)(
    //                                     staging: RDD[(K, (Option[V], Option[V]))] => S
    //                                     ): (RDD[(K, V)], RDD[(K, (V, V))], RDD[(K, V)], S) = {
    //
    //      val cogrouped = self.cogroup(other)
    //
    //      val mixed: RDD[(K, (Option[V], Option[V]))] = cogrouped.mapValues{
    //        tuple =>
    //          val leftOption = tuple._1.reduceLeftOption(innerReducer)
    //          val rightOption = tuple._2.reduceLeftOption(innerReducer)
    //
    //          (leftOption, rightOption)
    //      }
    //      val stage = staging(mixed)
    //
    //      val leftExclusive = mixed.flatMapValues {
    //        case (Some(left), None) => Some(left)
    //        case _ => None
    //      }
    //      val Intersection = mixed.flatMapValues {
    //        case (Some(left), Some(right)) => Some(left, right)
    //        case _ => None
    //      }
    //      val rightExclusive = mixed.flatMapValues {
    //        case (None, Some(right)) => Some(right)
    //        case _ => None
    //      }
    //      (leftExclusive, Intersection, rightExclusive, stage)
    //    }

    def unionByKey(
                    other: RDD[(K, V)])(
                    innerReducer: (V, V) => V
                  ): RDD[(K, V)] = {

      val cogrouped = self.cogroup(other)

      cogrouped.mapValues {
        tuple =>
          val reduced = (tuple._1 ++ tuple._2).reduce(innerReducer)
          reduced
      }
    }

    def intersectionByKey(
                           other: RDD[(K, V)])(
                           innerReducer: (V, V) => V
                         ): RDD[(K, V)] = {

      val cogrouped = self.cogroup(other)

      cogrouped.flatMap {
        triplet =>
          val tuple = triplet._2
          if (tuple._1.nonEmpty || tuple._2.nonEmpty) {
            val reduced = (tuple._1 ++ tuple._2).reduce(innerReducer)
            Some(triplet._1 -> reduced)
          }
          else {
            None
          }
      }
    }

    //TODO: remove, delegated to GenPartitioner
    //    def groupByKey_narrow(): RDD[(K, Iterable[V])] = {
    //
    //      self.mapPartitions{
    //        itr =>
    //          itr
    //            .toTraversable
    //            .groupBy(_._1)
    //            .map(v => v._1 -> v._2.map(_._2).toIterable)
    //            .iterator
    //      }
    //    }
    //    def reduceByKey_narrow(
    //                            reducer: (V, V) => V
    //                          ): RDD[(K, V)] = {
    //      self.mapPartitions{
    //        itr =>
    //          itr
    //            .toTraversable
    //            .groupBy(_._1)
    //            .map(v => v._1 -> v._2.map(_._2).reduce(reducer))
    //            .iterator
    //      }
    //    }
    //
    //    def groupByKey_beacon[T](
    //                              beaconRDD: RDD[(K, T)]
    //                            ): RDD[(K, Iterable[V])] = {
    //
    //      val cogrouped = self.cogroup(beaconRDD, beaconRDD.partitioner.get)
    //      cogrouped.mapValues {
    //        tuple =>
    //          tuple._1
    //      }
    //    }

    def reduceByKey_beacon[T](
                               reducer: (V, V) => V,
                               beaconRDD: RDD[(K, T)]
                             ): RDD[(K, V)] = {

      val cogrouped = self.cogroup(beaconRDD, beaconRDD.partitioner.get)
      cogrouped.mapValues {
        tuple =>
          tuple._1.reduce(reducer)
      }
    }
  }

  implicit class MapView[K, V](self: scala.collection.Map[K,V]) {

    def getTyped[T: ClassTag](key: K): Option[T] = self.get(key) match {

      case Some(res) =>
        res match {
          case r: T => Some(r)
          case _ => None
        }
      case _ => None
    }

    def flattenByKey(
                      key: K,
                      sampler: Sampler[Any]
                    ): Seq[(Map[K, Any], Int)] = {

      val valueOption: Option[V] = self.get(key)

      val values: Iterable[(Any, Int)] = valueOption.toIterable.flatMap(SpookyUtils.asIterable[Any]).zipWithIndex
      val sampled = sampler(values)

      val cleaned = self - key
      val result = sampled.toSeq.map(
        tuple =>
          (cleaned + (key -> tuple._1)) -> tuple._2
      )

      result
    }

    def canonizeKeysToColumnNames: scala.collection.Map[String,V] = self.map(
      tuple =>{
        val keyName: String = tuple._1 match {
          case symbol: scala.Symbol =>
            symbol.name //TODO: remove, this feature should no longer work after dataframe integration
          case _ =>
            tuple._1.toString
        }
        (SpookyUtils.canonizeColumnName(keyName), tuple._2)
      }
    )

    def sortBy[B: Ordering](fn: ((K, V)) => B): ListMap[K, V] = {
      val tuples = self.toList.sortBy(fn)
      ListMap(tuples: _*)
    }
  }

  implicit class TraversableLikeView[A, Repr](self: TraversableLike[A, Repr])(implicit ctg: ClassTag[A]) {

    def filterByType[B: ClassTag]: FilterByType[B] = new FilterByType[B]

    class FilterByType[B: ClassTag] {

      def get[That](implicit bf: CanBuildFrom[Repr, B, That]): That = {
        val result = self.flatMap{
          v =>
            SpookyUtils.typedOrNone[B](v)
        }(bf)
        result
      }
    }

    def mapToRDD[B: ClassTag](sc: SparkContext, local: Boolean = false, sliceOpt: Option[Int] = None)(
      f: A => B
    ): RDD[B] = {
      if (local) {
        sc.parallelize(
          self.toSeq.map(
            f
          ),
          sliceOpt.getOrElse(sc.defaultParallelism)
        )
      }
      else {
        sc.parallelize(
          self.toSeq,
          sliceOpt.getOrElse(sc.defaultParallelism)
        )
          .map(
            f
          )
      }
    }
  }

  implicit class ArrayView[A](self: Array[A]) {

    def filterByType[B <: A: ClassTag]: Array[B] = {
      self.flatMap {
        v =>
          SpookyUtils.typedOrNone[B](v)
      }
    }

    def flattenByIndex(
                        i: Int,
                        sampler: Sampler[Any]
                      ): Seq[(Array[Any], Int)]  = {

      val valueOption: Option[A] = if (self.indices contains i) Some(self.apply(i))
      else None

      val values: Iterable[(Any, Int)] = valueOption.toIterable.flatMap(SpookyUtils.asIterable[Any]).zipWithIndex
      val sampled = sampler(values)

      val result: Seq[(Array[Any], Int)] = sampled.toSeq.map{
        tuple =>
          val updated = self.updated(i, tuple._1)
          updated -> tuple._2
      }

      result
    }
  }

  implicit class DataFrameView(val self: DataFrame) {

    def toMapRDD(keepNull: Boolean = false): RDD[Map[String,Any]] = {
      val headers = self.schema.fieldNames

      val result: RDD[Map[String,Any]] = self.rdd.map{
        row => ListMap(headers.zip(row.toSeq): _*)
      }

      val filtered = if (keepNull) result
      else result.map {
        map =>
          map.filter(_._2 != null)
      }

      filtered
    }
  }

  //  implicit class TraversableOnceView[A, Coll[A] <: TraversableOnce[A], Raw](self: Raw)(implicit cast: Raw => Coll[A]) {
  //
  //    def filterByType[B: ClassTag]: Coll[B] = {
  //      val result = cast(self).flatMap{
  //        case tt: B => Some(tt)
  //        case _ => None
  //      }
  //      result.to[Coll[B]]
  //    }
  //  }

  //  implicit class ArrayView[A](self: Array[A]) {
  //
  //    def filterByType[B <: A: ClassTag]: Array[B] = {
  //      val result: Array[B] = self.flatMap{
  //        case tt: B => Some(tt)
  //        case _ => None
  //      }
  //      result
  //    }
  //  }

  //  implicit class TraversableLikeView[+A, +Repr, Raw](self: Raw)(implicit cast: Raw => TraversableLike[A,Repr]) {
  //
  //    def filterByType[B] = {
  //      val v = cast(self)
  //      val result = v.flatMap{
  //        case tt: B => Some(tt)
  //        case _ => None
  //      }
  //      result
  //    }
  //  }

  implicit class StringView(str: String) {

    def :/(other: String): String = SpookyUtils./:/(str, other)
    def \\(other: String): String = SpookyUtils.\\\(str, other)

    def interpolate(delimiter: String)(
      replace: String => String
    ): String = {

      if (str == null || str.isEmpty) return str

      val specialChars = "(?=[]\\[+$&|!(){}^\"~*?:\\\\-])"
      val escaped = delimiter.replaceAll(specialChars, "\\\\")
      val regex = ("(?<!" + escaped + ")" + escaped + "\\{[^\\{\\}\r\n]*\\}").r

      val result = regex.replaceAllIn(
        str,
        m => {
          val original = m.group(0)
          val key = original.substring(2, original.length - 1)

          val replacement = replace(key)
          replacement
        })
      result
    }
  }
}
