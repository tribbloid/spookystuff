package com.tribbloids.spookystuff

import com.esotericsoftware.kryo.Kryo
import com.tribbloids.spookystuff.conf.{Dir, SpookyConf}
import com.tribbloids.spookystuff.doc.*
import com.tribbloids.spookystuff.metrics.SpookyMetrics
import com.tribbloids.spookystuff.row.AgentRow
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.immutable.ListMap
import scala.concurrent.duration.FiniteDuration

//TODO: not all classes are registered, need an adaptive codegen to register them automatically
class SpookyKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    val array: Array[Class[?]] = Array(
      // used by PageRow
      classOf[TypeTag[?]],
      classOf[AgentRow[?]],
      classOf[ListMap[?, ?]],
      classOf[UUID],
      classOf[Elements[?]],
      classOf[Siblings[?]],
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
      classOf[TimeUnit]
    )
    array.foreach(kryo.register)
  }
}
