package com.tribbloids.spookystuff

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.Kryo
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.row.FetchedRow
import org.apache.spark.SerializableWritable
import org.apache.spark.serializer.KryoRegistrator

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
      classOf[FetchedRow],
      classOf[ListMap[_,_]],
      classOf[UUID],
      classOf[Elements[_]],
      classOf[Siblings[_]],
      classOf[HtmlElement],
      classOf[JsonElement],
      classOf[Doc],
//      classOf[UnknownElement],
//      classOf[ExploreStage],

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
      FilePaths.getClass,
      PartitionerFactories.getClass,
      ProxyFactories.getClass
    )
    array.foreach(kryo.register)
  }
}
