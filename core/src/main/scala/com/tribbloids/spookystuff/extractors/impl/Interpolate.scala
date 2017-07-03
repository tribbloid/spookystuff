package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.extractors.GenExtractor.StaticType
import com.tribbloids.spookystuff.extractors._
import org.apache.spark.sql.types._

/**
  * Created by peng on 7/3/17.
  */
case class Interpolate(parts: Seq[String], _args: Seq[Extractor[Any]]) extends Extractor[String] with StaticType[FR, String] {

  override def resolve(tt: DataType): PartialFunction[FR, String] = {
    val rs = _args.map(_.resolve(tt).lift)

    Unlift({
      row =>
        val iParts = parts.map(row.dataRow.replaceInto(_))

        val vs = rs.map(_.apply(row))
        val result = if (iParts.contains(None) || vs.contains(None)) None
        else Some(iParts.zip(vs).map(tpl => tpl._1.get + tpl._2.get).mkString + iParts.last.get)

        result
    })
  }

  override val dataType: DataType = StringType
}
