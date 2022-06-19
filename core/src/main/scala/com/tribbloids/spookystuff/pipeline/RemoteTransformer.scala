package com.tribbloids.spookystuff.pipeline

import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.SpookyContext
import org.apache.spark.ml.param.{Param, ParamMap, ParamPair}
import org.slf4j.LoggerFactory

import scala.language.dynamics

trait RemoteTransformer extends RemoteTransformerLike with Dynamic {

  import com.tribbloids.spookystuff.dsl._

//  override val uid = this.getClass.getCanonicalName + "_" + UUID.randomUUID().toString

  override def copy(extra: ParamMap): RemoteTransformer = this.defaultCopy(extra)

  def +>(another: RemoteTransformer): RemoteTransformerChain = new RemoteTransformerChain(Seq(this)) +> another

  /*
  This dynamic function automatically add a setter to any Param-typed property
   */
  def applyDynamic(methodName: String)(args: Any*): this.type = {
    assert(args.length == 1)
    val arg = args.head

    if (methodName.startsWith("set")) {
      val fieldName = methodName.stripPrefix("set")
      val fieldOption = this.params.find(_.name == fieldName)

      fieldOption match {
        case Some(field) =>
          set(field.asInstanceOf[Param[Any]], arg)
        case None =>
          throw new IllegalArgumentException(s"parameter $fieldName doesn't exist")
        //          dynamicParams.put(fieldName, arg)
      }

      this
    } else throw new IllegalArgumentException(s"function $methodName doesn't exist")
  }

  //example value of parameters used for testing
  val exampleParamMap: ParamMap = ParamMap.empty

  def exampleInput(spooky: SpookyContext): FetchedDataset = spooky

  protected final def setExample(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      setExample(p.param.asInstanceOf[Param[Any]], p.value)
    }
    this
  }

  protected final def setExample[T](param: Param[T], value: T): this.type = {
    exampleParamMap.put(param -> value)
    this
  }

  override def explainParam(param: Param[_]): String = {
    val str = super.explainParam(param)

    val exampleStr = if (isDefined(param)) {
      exampleParamMap.get(param).map("(e.g.: " + _ + " )")
    } else {
      ""
    }
    str + exampleStr
  }

  override def test(spooky: SpookyContext): Unit = {

    val names = this.params.map(_.name)
    assert(names.length == names.distinct.length) //ensure that there is no name duplicity

    this.exampleParamMap.toSeq.foreach { pair =>
      this.set(pair)
    }

    val result: FetchedDataset = this.transform(this.exampleInput(spooky)).persist()
    val keys = result.fields

    result.toDF(sort = true).show()

    keys.foreach { key =>
      val distinct = result.rdd.flatMap(_.dataRow.get(key)).distinct()
      val values = distinct.take(2)
      assert(values.length >= 1)
      if (key.isDepth) {
        key.depthRangeOpt.foreach { range =>
          assert(range.max == distinct.map(_.asInstanceOf[Int]).max())
          assert(range.min == distinct.map(_.asInstanceOf[Int]).min())
        }
      }
      LoggerFactory.getLogger(this.getClass).info(s"column '${key.name} has passed the test")
      result.unpersist()
    }

    assert(result.toObjectRDD(S_*).flatMap(v => v.originalSeq).count() >= 1)
  }
}
