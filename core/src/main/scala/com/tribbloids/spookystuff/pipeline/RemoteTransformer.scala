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

  def +> (another: RemoteTransformer): RemoteTransformerChain = new RemoteTransformerChain(Seq(this)) +> another

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
    }
    else throw new IllegalArgumentException(s"function $methodName doesn't exist")
  }

  /*
  TODO: the original plan of using dynamic param definition to reduce pipeline code seems not to be supported by scala, suspended
  see http://stackoverflow.com/questions/33699836/in-scala-how-to-find-invocation-of-subroutines-defined-in-a-function for detail
   */
  //  val dynamicParams: mutable.Map[String, Any] = mutable.Map()

  //  def param[T](
  //                name: String,
  //                default: T = null,
  //                example: T = null,
  //                defaultOption: Option[T] = None,
  //                exampleOption: Option[T] = None
  //              ): T = {
  //
  //    val paramOption = params.find(_.name == name)
  //    paramOption match {
  //      case Some(param) =>
  //        param
  //      case None =>
  //
  //    }
  //
  //    if (!params.exists(_.name == name)) {
  //      val param = new Param[T](this, name, doc)
  //      val effectiveDefault = Option(default).orElse(defaultOption)
  //      if (effectiveDefault.nonEmpty) this.setDefault(param -> effectiveDefault.get)
  //      val effectiveExample = Option(example).orElse(exampleOption)
  //      if (effectiveExample.nonEmpty) this.setExample(param -> effectiveExample.get)
  //      this.dynamicParams += param
  //      this.getOrDefault(param)
  //    }
  //    else {
  //      val param = params.filter(_.name == name).head.asInstanceOf[Param[T]]
  //      assert(param.doc == doc, "documentation has to be consistent")
  //      assert(this.getDefault(param) == defaultOption, "default value has to be consistent")
  //      assert(this.getExample(param) == exampleOption, "default value has to be consistent")
  //      this.getOrDefault[T](param)
  //    }
  //  }

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

  override def test(spooky: SpookyContext): Unit= {

    val names = this.params.map(_.name)
    assert(names.length == names.distinct.length) //ensure that there is no name duplicity

    this.exampleParamMap.toSeq.foreach {
      pair =>
        this.set(pair)
    }

    val result: FetchedDataset = this.transform(this.exampleInput(spooky)).persist()
    val keys = result.fields

    result.toDF(sort = true).show()

    keys.foreach{
      key =>
        val distinct = result.flatMap(_.dataRow.get(key)).distinct()
        val values = distinct.take(2)
        assert(values.length >= 1)
        if (key.isDepth) {
          key.depthRangeOpt.foreach {
              range =>
                assert(range.max == distinct.map(_.asInstanceOf[Int]).max())
                assert(range.min == distinct.map(_.asInstanceOf[Int]).min())
            }
        }
        LoggerFactory.getLogger(this.getClass).info(s"column '${key.name} has passed the test")
        result.unpersist()
    }

    assert(result.toObjectRDD(S_*).flatMap(identity).count() >= 1)
  }
}