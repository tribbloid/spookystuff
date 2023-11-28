package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors.GenExtractor.StaticType
import com.tribbloids.spookystuff.extractors._
import org.apache.spark.sql.types._

/**
  * Created by peng on 7/3/17.
  */
case class Interpolate(parts: Seq[String], _args: Seq[Extractor[Any]])
    extends Extractor[String]
    with StaticType[FR, String] {

  override def resolve(tt: DataType): PartialFunction[FR, String] = {
    val rs = _args.map(_.resolve(tt).lift)

    Unlift { row =>
      val vs = rs.map(_.apply(row))
      val result =
        if (vs.contains(None)) None
        else Some(parts.zip(vs).map(tpl => tpl._1 + tpl._2.get).mkString + parts.last)

      result
    }
  }

  override val dataType: DataType = StringType
}
