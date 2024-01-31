package org.apache.spark.ml.dsl.utils.refl

import com.tribbloids.spookystuff.testutils.BaseSpec
import org.apache.spark.sql.catalyst.ScalaReflection

import scala.collection.immutable

class TypeSpike extends BaseSpec {

  import ScalaReflection.universe._

  it("Map type equality") {
    val ttg1 = typeTag[Map[_, _]]
    val ttg2 = typeTag[immutable.Map[_, _]]

    assert(ttg1.tpe =:= ttg2.tpe)
  }

  it("List type equality") {
    val ttg1 = typeTag[List[_]]
    val ttg2 = typeTag[immutable.List[_]]

    assert(ttg1.tpe =:= ttg2.tpe)
  }
}
