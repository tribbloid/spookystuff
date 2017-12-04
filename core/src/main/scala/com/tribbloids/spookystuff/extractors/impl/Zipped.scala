package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors.{Extractor, FR, GenExtractor, Unlift}
import org.apache.spark.ml.dsl.utils.refl.ScalaType._
import org.apache.spark.sql.types._

import scala.collection.immutable.ListMap

/**
  * Created by peng on 7/3/17.
  */
//TODO: delegate to And_->
//TODO: need tests
case class Zipped[T1,+T2](
                           arg1: Extractor[Iterable[T1]],
                           arg2: Extractor[Iterable[T2]]
                         )
  extends Extractor[Map[T1, T2]] {

  override val _args: Seq[GenExtractor[FR, _]] = Seq(arg1, arg2)

  override def resolveType(tt: DataType): DataType = {
    val t1 = arg1.resolveType(tt).unboxArrayOrMap
    val t2 = arg2.resolveType(tt).unboxArrayOrMap

    MapType(t1, t2)
  }

  override def resolve(tt: DataType): PartialFunction[FR, Map[T1, T2]] = {
    val r1 = arg1.resolve(tt).lift
    val r2 = arg2.resolve(tt).lift

    Unlift({
      row =>
        val z1Option = r1.apply(row)
        val z2Option = r2.apply(row)

        if (z1Option.isEmpty || z2Option.isEmpty) None
        else {
          val map: ListMap[T1, T2] = ListMap(z1Option.get.toSeq.zip(
            z2Option.get.toSeq
          ): _*)

          Some(map)
        }
    })
  }
}
