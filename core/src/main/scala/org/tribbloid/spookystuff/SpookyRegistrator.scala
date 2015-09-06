package org.tribbloid.spookystuff

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SerializableWritable
import org.apache.spark.serializer.KryoRegistrator
import org.tribbloid.spookystuff.SpookyConf.DirConf
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.entity.{ExploreStage, PageRow}
import org.tribbloid.spookystuff.pages._

import scala.collection.immutable.ListMap
import scala.concurrent.duration.FiniteDuration

/**
 * Created by peng on 4/22/15.
 */
//TODO: not all classes are registered which renders this class useless
class SpookyRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    val array: Array[Class[_]] = Array(
      //used by PageRow
      classOf[PageRow],
      classOf[ListMap[_,_]],
      classOf[UUID],
      classOf[Elements[_]],
      classOf[Siblings[_]],
      classOf[HtmlElement],
      classOf[JsonElement],
      classOf[Page],
//      classOf[UnknownElement],
      classOf[ExploreStage],

      //used by broadcast & accumulator
      classOf[SpookyConf],
      classOf[DirConf],
      classOf[SerializableWritable[_]],
      classOf[SpookyContext],
      classOf[Metrics],

      //used by Expressions
      //      classOf[NamedFunction1]

      //parameters
      classOf[FiniteDuration],
      classOf[TimeUnit],
      PageFilePaths.getClass,
      CacheFilePaths.getClass,
      Parallelism.getClass,
      ProxyFactories.getClass
    )
    array.foreach(kryo.register)
  }
}
