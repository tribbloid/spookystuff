package org.tribbloid.spookystuff

import java.util.UUID

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SerializableWritable
import org.apache.spark.serializer.KryoRegistrator
import org.tribbloid.spookystuff.SpookyConf.Dirs
import org.tribbloid.spookystuff.entity.{ExploreStage, PageRow}
import org.tribbloid.spookystuff.pages._

import scala.collection.immutable.ListMap
import scala.concurrent.duration.FiniteDuration

/**
 * Created by peng on 4/22/15.
 */
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
      classOf[Page],
      classOf[UnknownElement],
      classOf[ExploreStage],

      //used by broadcast & accumulator
      classOf[SpookyConf],
      classOf[Dirs],
      classOf[SerializableWritable[_]],
      classOf[SpookyContext],
      classOf[Metrics],

      //used by Expressions
      //      classOf[NamedFunction1]

      //parameters
      classOf[FiniteDuration]
    )
    array.foreach(kryo.register)
  }
}
