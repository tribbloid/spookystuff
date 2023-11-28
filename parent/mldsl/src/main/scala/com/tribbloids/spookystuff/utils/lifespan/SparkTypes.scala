package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession

trait SparkTypes {

  trait SparkType extends LeafType with NOTSerializable {

    def sparkContext: SparkContext

    {
      sparkContext
    }

    type ID = String
  }

  case class SparkApp(
      override val sparkContext: SparkContext = SparkSession.active.sparkContext
  ) extends SparkType {

    class Listener(fn: () => Unit) extends SparkListener with Serializable {

      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {

        fn()
      }
    }

    override protected def _batchID(ctx: LifespanContext): ID = sparkContext.applicationId

    override protected def _registerHook(ctx: LifespanContext, fn: () => Unit): Unit =
      sparkContext.addSparkListener(new Listener(fn))
  }

  object ActiveSparkApp extends SparkApp(SparkSession.active.sparkContext) {}

  // TODO: add Job impl
}
