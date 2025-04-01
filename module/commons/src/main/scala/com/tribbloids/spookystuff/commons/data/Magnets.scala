package com.tribbloids.spookystuff.commons.data

import scala.language.implicitConversions

object Magnets {

  case class AttrValueMag[+T](
      k: String,
      vOpt: Option[T]
  )

  object AttrValueMag {

    implicit def fromKeyValue[T](kv: (String, T)): AttrValueMag[T] = AttrValueMag(kv._1, Some(kv._2))
    implicit def fromAttrValue[T](kv: (AttrLike[T], T)): AttrValueMag[T] = AttrValueMag(kv._1.name, Some(kv._2))

// fuck scala
    implicit def fromKeyValues[T](kv: Seq[(String, T)]): Seq[AttrValueMag[T]] = kv.map(v => v)
    implicit def fromAttrValues[T](kv: Seq[(AttrLike[T], T)]): Seq[AttrValueMag[T]] = kv.map(v => v)

    implicit def fromItr[T, Src](kvs: Iterable[Src])(
        implicit
        ev: Src => AttrValueMag[T]
    ): Iterable[AttrValueMag[T]] = kvs.map(ev)
  }

  case class AttrMag(names: Seq[String])

  object AttrMag {

    implicit def fromStr(v: String): AttrMag = AttrMag(Seq(v))
    implicit def fromAttr(v: AttrLike[?]): AttrMag = AttrMag(v.allNames)

    implicit def fromItr[Src](kss: Iterable[Src])(
        implicit
        ev: Src => AttrMag
    ): Iterable[AttrMag] = kss.map(ev)
  }
}
