package com.tribbloids.spookystuff.utils

import java.sql.{Date, Timestamp}

import com.tribbloids.spookystuff.row._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.ListMap
import scala.collection.{Map, TraversableLike}
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

/**
  * Created by peng on 11/7/14.
  * implicit conversions in this package are used for development only
  */
object SpookyViews {

  val SPARK_JOB_DESCRIPTION = "spark.job.description"
  val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"
  val RDD_SCOPE_KEY = "spark.rdd.scope"
  val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"

  implicit class SparkContextView(val self: SparkContext) {

    def withJob[T](description: String)(fn: T): T = {

      val oldDescription = self.getLocalProperty(SPARK_JOB_DESCRIPTION)
      if (oldDescription == null) self.setJobDescription(description)
      else self.setJobDescription(oldDescription + " > " + description)

      val result: T = fn
      self.setJobGroup(null,oldDescription)
      result
    }

    def exePerCore[T: ClassTag](f: => T): RDD[T] = {
      self.parallelize(1 to self.defaultParallelism * 4)
        .mapPartitions(
          itr =>
            Iterator(f)
        )
    }

    def exePerWorker[T](f: () => T): RDD[T] = ???
  }

  implicit class RDDView[T](val self: RDD[T]) {

    def collectPerPartition: Array[List[T]] = self.mapPartitions(v => Iterator(v.toList)).collect()

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

    //  def checkpointNow(): Unit = {
    //    persistDuring(StorageLevel.MEMORY_ONLY) {
    //      self.checkpoint()
    //      self.foreach(_ =>)
    //      self
    //    }
    //    Unit
    //  }
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

  implicit class MapView[K, V](m1: scala.collection.Map[K,V]) {

    def getTyped[T: ClassTag](key: K): Option[T] = m1.get(key) match {

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

      val valueOption: Option[V] = m1.get(key)

      val values: Iterable[(Any, Int)] = valueOption.toIterable.flatMap(SpookyUtils.asIterable[Any]).zipWithIndex
      val sampled = sampler(values)

      val cleaned = m1 - key
      val result = sampled.toSeq.map(
        tuple =>
          (cleaned + (key -> tuple._1)) -> tuple._2
      )

      result
    }

    def canonizeKeysToColumnNames: scala.collection.Map[String,V] = m1.map(
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
  }

  implicit class TraversableLikeView[A, Repr](self: TraversableLike[A, Repr]) {

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
  }

  import ScalaReflection.universe._

  lazy val atomicExamples: Seq[(Any, TypeTag[_])] = {

    implicit def pairFor[T: TypeTag](v: T): (T, TypeTag[T]) = {
      v -> TypeUtils.getTypeTag[T](v)
    }

    val result = Seq[(Any, TypeTag[_])](
      Array(0: Byte),
      false,
      new Date(0),
      new Timestamp(0),
      0.0,
      0: Float,
      0: Byte,
      0: Int,
      0L,
      0: Short,
      "a"
    )
    result
  }

  lazy val atomicTypePairs: Seq[(DataType, TypeTag[_])] = atomicExamples.map {
    v =>
      v._2.catalystType -> v._2
  }

  lazy val atomicTypeMap: Map[DataType, TypeTag[_]] = {
    Map(atomicTypePairs: _*)
  }

  implicit class DataTypeView(tt: DataType) {

    // CatalystType => ScalaType
    // used in ReflectionMixin to determine the exact function to:
    // 1. convert data from CatalystType to canonical Scala Type (and obtain its TypeTag)
    // 2. use the obtained TypeTag to get the specific function implementation and applies to the canonic Scala Type data.
    // 3. get the output TypeTag of the function, use it to generate the output DataType of the new Extraction.
    def scalaTypeOpt: Option[TypeTag[_]] = {

      tt match {
        case NullType =>
          Some(TypeTag.Null)
        case st: ScalaType =>
          Some(st.ttg)
        case t if atomicTypeMap.contains(t) =>
          atomicTypeMap.get(t)
        case ArrayType(inner, _) =>
          val innerTagOpt = inner.scalaTypeOpt
          innerTagOpt.map {
            case at: TypeTag[a] =>
              implicit val att = at
              typeTag[Array[a]]
          }
        case MapType(key, value, _) =>
          val keyTag = key.scalaTypeOpt
          val valueTag = value.scalaTypeOpt
          val pairs = (keyTag, valueTag) match {
            case (Some(kt), Some(vt)) => Some(kt -> vt)
            case _ => None
          }

          pairs.map {
            pair =>
              (pair._1, pair._2) match {
                case (ttg1: TypeTag[a], ttg2: TypeTag[b]) =>
                  implicit val t1 = ttg1
                  implicit val t2 = ttg2
                  typeTag[Map[a, b]]
              }
          }
        case _ =>
          None
      }
    }

    def scalaType: TypeTag[_] = {
      scalaTypeOpt.getOrElse {
        throw new UnsupportedOperationException(s"cannot convert Catalyst type $tt to Scala type")
      }
    }

    def reify = {

      val result = UnreifiedScalaType.reify(tt)
      result
    }

    def unboxArrayOrMap: DataType = {
      tt._unboxArrayOrMapOpt
        .orElse(
          tt.reify._unboxArrayOrMapOpt
        )
        .getOrElse(
          throw new UnsupportedOperationException(s"Type $tt is not an Array")
        )
    }

    private[utils] def _unboxArrayOrMapOpt: Option[DataType] = {
      tt match {
        case ArrayType(boxed, _) =>
          Some(boxed)
        case MapType(keyType, valueType, valueContainsNull) =>
          Some(StructType(Array(
            StructField("_1", keyType),
            StructField("_2", valueType, valueContainsNull)
          )))
        case _ =>
          None
      }
    }

    def filterArray: Option[DataType] = {
      if (tt.reify.isInstanceOf[ArrayType])
        Some(tt)
      else
        None
    }

    def asArray: DataType = {
      filterArray.getOrElse{
        ArrayType(tt)
      }
    }

    def ensureArray: DataType = {
      filterArray.getOrElse{
        throw new UnsupportedOperationException(s"Type $tt is not an Array")
      }
    }

    def =~= (another: DataType): Boolean = {
      val result = (tt eq another) ||
        (tt == another) ||
        (tt.reify == another.reify)

      result
    }

    def =~=!(another: DataType): Unit = {
      val result = =~= (another)
      assert (
        result,
        s"""
           |Type not equal:
           |LEFT:  $tt -> ${tt.reify}
           |RIGHT: $another -> ${another.reify}
          """.stripMargin
      )
    }
  }

  implicit class TypeTagView[T](ttg: TypeTag[T]) {

    def catalystTypeOpt: Option[DataType] = {
      TypeUtils.catalystTypeOptFor(ttg)
    }

    def catalystType: DataType = {
      TypeUtils.catalystTypeFor(ttg)
    }

    def classTag: ClassTag[T] = ClassTag(_clz)

    def clazz: Class[T] = _clz.asInstanceOf[Class[T]]

    //FIXME: cannot handle Array[_]
    private def _clz: ScalaReflection.universe.RuntimeClass = {
      val tpe = ttg.tpe
      ttg.mirror.runtimeClass(tpe)
    }
  }

  implicit class ClassTagViews[T](self: ClassTag[T]) {

    //  def toTypeTag: TypeTag[T] = TypeTag.apply()

    def clazz: Class[T] = self.runtimeClass.asInstanceOf[Class[T]]
  }

  implicit class ClassView[T](self: Class[T]) {

    val m = runtimeMirror(self.getClassLoader)

    def toType: Type = {

      val classSymbol = m.staticClass(self.getCanonicalName)
      val tpe = classSymbol.selfType
      tpe
    }

    def toTypeTag: TypeTag[T] = {
      TypeUtils.createTypeTag(toType, m)
    }
  }
}
