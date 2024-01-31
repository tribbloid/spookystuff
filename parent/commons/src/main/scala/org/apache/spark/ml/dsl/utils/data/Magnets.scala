package org.apache.spark.ml.dsl.utils.data

import scala.language.implicitConversions

object Magnets {

  case class AttrValueMag[+T](
      k: String,
      vOpt: Option[T]
  )

  object AttrValueMag {

    implicit def fromTuple1[T](kv: (String, T)): AttrValueMag[T] = AttrValueMag(kv._1, Some(kv._2))
    implicit def fromTuple2[T](kv: (AttrLike[T], T)): AttrValueMag[T] = AttrValueMag(kv._1.primaryName, Some(kv._2))

    implicit def fromItr[T, Src](kvs: Iterable[Src])(
        implicit
        ev: Src => AttrValueMag[T]
    ): Iterable[AttrValueMag[T]] = kvs.map(ev)
  }

  case class AttrMag(names: Seq[String])

  object AttrMag {

    implicit def fromStr(v: String): AttrMag = AttrMag(Seq(v))
    implicit def fromAttr(v: AttrLike[_]): AttrMag = AttrMag(v.allNames)

    implicit def fromItr[Src](kss: Iterable[Src])(
        implicit
        ev: Src => AttrMag
    ): Iterable[AttrMag] = kss.map(ev)
  }
}
