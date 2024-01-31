package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.metrics.{AbstractMetrics, Acc}
import com.tribbloids.spookystuff.agent.PythonDriver
import org.apache.spark.SparkConf
import org.apache.spark.util.LongAccumulator

object Python extends PluginSystem.HasDriver {

  final val DEFAULT_PYTHONDRIVER_FACTORY: DriverFactory.TaskLocal[PythonDriver] = PythonDriverFactory._3.taskLocal

  case class Conf(
      pythonDriverFactory: DriverFactory[PythonDriver] = DEFAULT_PYTHONDRIVER_FACTORY
  ) extends ConfLike {

    override def importFrom(sparkConf: SparkConf): Python.Conf = this.copy()
  }
  def defaultConf: Conf = Conf()

  case class Metrics(
      pythonInterpretationSuccess: Acc[LongAccumulator] = Acc.create(0L, "pythonInterpretationSuccess"),
      pythonInterpretationError: Acc[LongAccumulator] = Acc.create(0L, "pythonInterpretationSuccess")
  ) extends AbstractMetrics {}

  type Driver = PythonDriver

  case class Plugin(
      spooky: SpookyContext,
      @transient effectiveConf: Python.Conf,
      override val metrics: Metrics = Metrics()
  ) extends PluginLike {

    override def driverFactory: DriverFactory[PythonDriver] = getConf.pythonDriverFactory

    /**
      * only swap out configuration do not replace anything else
      */
    override def withEffectiveConf(conf: Python.Conf): Plugin = copy(effectiveConf = conf)
  }

  implicitly[this.Driver =:= PythonDriver]

  override def default(spooky: SpookyContext): Plugin =
    Plugin(spooky, defaultConf).withConf(defaultConf)
}
