package com.tribbloids.spookystuff.commons.refl

import com.tribbloids.spookystuff.testutils.BaseSpec
import org.apache.spark.sql.catalyst.ScalaReflection

import scala.collection.immutable

class TypeSpike extends BaseSpec {

  import ScalaReflection.universe.*

  it("Map type equality") {
    val ttg1 = typeTag[Map[?, ?]]
    val ttg2 = typeTag[immutable.Map[?, ?]]

    assert(ttg1.tpe =:= ttg2.tpe)
  }

  it("List type equality") {
    val ttg1 = typeTag[List[?]]
    val ttg2 = typeTag[immutable.List[?]]

    assert(ttg1.tpe =:= ttg2.tpe)
  }
}
