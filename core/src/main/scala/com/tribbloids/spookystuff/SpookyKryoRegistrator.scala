package com.tribbloids.spookystuff

import com.esotericsoftware.kryo.Kryo
import com.tribbloids.spookystuff.conf.{Dir, SpookyConf}
import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.metrics.SpookyMetrics
import com.tribbloids.spookystuff.row.FetchedRow
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.immutable.ListMap
import scala.concurrent.duration.FiniteDuration

//TODO: not all classes are registered which renders this class useless
// should be part of the Plugin API
class SpookyKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    val array: Array[Class[_]] = Array(
      // used by PageRow
      classOf[TypeTag[_]],
      classOf[FetchedRow],
      classOf[ListMap[_, _]],
      classOf[UUID],
      classOf[Elements[_]],
      classOf[Siblings[_]],
      classOf[HtmlElement],
      classOf[JsonElement],
      classOf[Doc],
//      classOf[UnknownElement],
//      classOf[ExploreStage],

      // used by broadcast & accumulator
      classOf[SpookyConf],
      classOf[Dir.Conf],
      classOf[SpookyContext],
      classOf[SpookyMetrics],
      // used by Expressions
      //      classOf[NamedFunction1]

      // parameters
      classOf[FiniteDuration],
      classOf[TimeUnit],
      FilePaths.getClass,
      PartitionerFactories.getClass
    )
    array.foreach(kryo.register)
  }
}
