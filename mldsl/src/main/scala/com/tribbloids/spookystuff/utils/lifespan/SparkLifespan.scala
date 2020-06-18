package com.tribbloids.spookystuff.utils.lifespan

import com.tribbloids.spookystuff.utils.lifespan.Lifespan.LifespanType
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession

trait SparkLifespan extends LifespanType {

  @transient lazy val sc: SparkContext = SparkSession.active.sparkContext

  {
    sc
  }

  case class ID(id: String) {
    override def toString: String = s"Spark.$productPrefix-$id"
  }
}
object SparkLifespan {

  case object App extends SparkLifespan {

    class Listener(fn: () => Unit) extends SparkListener with Serializable {

      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {

        fn()
      }
    }
  }
  case class App(
      override val nameOpt: Option[String] = None,
      ctxFactory: () => LifespanContext = () => LifespanContext()
  ) extends Lifespan {
    def this() = this(None)

    import App._

    override def getBatchIDs = Seq(ID(sc.applicationId))

    override def registerHook(fn: () => Unit): Unit = {

      sc.addSparkListener(new Listener(fn))
    }
  }

  // TODO: add Job impl
}
