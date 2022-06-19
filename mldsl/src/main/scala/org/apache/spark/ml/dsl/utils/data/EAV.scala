package org.apache.spark.ml.dsl.utils.data

import java.util.Properties

import com.tribbloids.spookystuff.utils.{CommonUtils, IDMixin, TreeThrowable}
import org.apache.spark.ml.dsl.utils.{?, ScalaNameMixin}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Try

/**
  * entity-(with)-attribute-value
  */
trait EAV extends Serializable with IDMixin {

  type VV
  protected def getCtg(implicit v: ClassTag[VV]) = v
  def ctg: ClassTag[VV]
  final lazy val _ctg = ctg

  def source: EAV

  final lazy val core: EAV.Impl = source match {
    case c: EAV.Impl => c
    case _           => source.core
  }

  lazy val asOriginalMap: ListMap[String, VV] = {

    core.self.collect {
      case (k, _ctg(v)) => k -> v
      case (k, null)    => k -> null.asInstanceOf[VV]
    }
  }
  lazy val asCaseInsensitiveMap: CaseInsensitiveMap[VV] = CaseInsensitiveMap(asOriginalMap)

  def asMap: Map[String, VV] = asOriginalMap
  def asStrMap: Map[String, String] = asMap.mapValues(v => Option(v).map(_.toString).orNull)

  //TODO: change to declaredAttrs?
  override def _id: Any = asMap

  def asProperties: Properties = {
    val properties = new Properties()

    asStrMap.foreach { v =>
      properties.put(v._1, v._2)
    }

    properties
  }

  /**
    * favor the key-value pair in first operand
    * attempt to preserve sequence as much as possible
    */
  def :++(other: EAV): EAV.Impl = {

    EAV.Impl.fromMap(CommonUtils.mergePreserveOrder(this.core.self, other.core.self))
  }

  /**
    * favor the key-value pair in second operand
    * operands suffixed by : are reversed
    */
  final def ++:(other: EAV): EAV.Impl = {

    :++(other)
  }

  //TODO: move to superclass
  final def +=+(
      other: EAV,
      include: List[Any] = Nil
  ): EAV.Impl = {

    val _include: List[String] = if (include.isEmpty) {
      (this.asMap.keys ++ other.asMap.keys).toList
    } else {
      include.flatMap {
        case v: this.Attr[_] => v.allNames
        case v: String       => Seq(v)
        case v @ _ =>
          throw new UnsupportedOperationException(s"unsupported key type for $v")
      }
    }

    val result: Seq[(String, Any)] = _include.flatMap { key =>
      val vs = Seq(this, other).map { v =>
        v.asMap.get(key)
      }.flatten

      val mergedOpt = vs match {
        case Seq(v1, v2) =>
          require(v1 == v2, s"cannot merge, diverging values for $key: $v1 != $v2")
          vs.headOption
        case _ =>
          vs.headOption
      }

      mergedOpt.map { merged =>
        key -> merged
      }
    }

    EAV.Impl.fromUntypedTuples(result: _*)
  }

  def updated(kvs: Magnets.KV[VV]*): EAV.Impl = {
    this ++: EAV(kvs: _*)
  }

  def updateIfExists(kvOpts: Magnets.KVOpt[VV]*): EAV.Impl = {

    updated(kvOpts.map(_.asKV): _*)
  }

  def defaultSeparator = " "
  def defaultKQuote = ""
  def defaultVQuote = ""

  def formattedStr(
      sep: String = defaultSeparator,
      kQuote: String = defaultKQuote,
      vQuote: String = defaultVQuote
  ): String = {
    asStrMap.map(tuple => s"$kQuote${tuple._1}$kQuote=$vQuote${tuple._2}$vQuote").mkString(sep)
  }

  lazy val showStr = formattedStr()
  lazy val showStr_unquoted = formattedStr(vQuote = "")

  lazy val providedHintStr: Option[String] = {
    if (asStrMap.isEmpty) {
      None
    } else {
      Some(s"only ${asMap.keys.mkString(", ")} are provided")
    }
  }

  //TODO: cleanup for being too redundant! not encouraged to use

  def tryGet(k: String, nullable: Boolean = false): Try[VV] = Try {
    val result = asMap.getOrElse(
      k,
      throw new UnsupportedOperationException(
        (
          Seq(
            s"Parameter $k is missing"
          ) ++ providedHintStr
        ).mkString("\n")
      )
    )
    if (!nullable) require(result != null, s"null value for `$k`")
    result
  }

  def get(k: String, nullable: Boolean = false): Option[VV] = tryGet(k, nullable).toOption

  def apply(k: String, nullable: Boolean = false): VV = tryGet(k, nullable).get

  def getOrElse(k: String, default: VV): VV = {
    require(
      default != null,
      s"default value for `$k` cannot be null"
    )
    get(k).getOrElse(default)
  }

  def contains(k: String): Boolean = tryGet(k).isSuccess

  def attr(v: String) = new Attr_(primaryNameOverride = v)

  def drop(vs: Magnets.K*): EAV.Impl = EAV.Impl.fromMap(asMap -- vs.flatMap(_.names))

  def dropAll(vs: Iterable[Magnets.K]): EAV.Impl = drop(vs.toSeq: _*)

  def --(vs: Iterable[Magnets.K]): EAV.Impl = dropAll(vs)

  //TODO: support mixing param and map definition? While still being serializable?
  class Attr[T](
      // should only be used in setters
      val aliases: List[String] = Nil,
      nullable: Boolean = false,
      default: T `?` _ = None,
      primaryNameOverride: String `?` _ = None
  )(implicit ev: T <:< VV)
      extends AttrLike[T]
      with ScalaNameMixin {

    override def toString: String = {
      throw new UnsupportedOperationException(
        "Attribute cannot be used as string, please use primaryName or value instead"
      )
    }

    def outer: EAV = EAV.this

    final def primaryName: String = primaryNameOverride.getOrElse(objectSimpleName)

    def explicitValue: T = {
      val trials: Seq[() => T] = allNames.map { name =>
        { () =>
          outer.apply(name, nullable).asInstanceOf[T]
        }
      }

      TreeThrowable
        .|||^(trials)
        .get
    }

    def defaultValue: T = default.getOrElse {
      throw new UnsupportedOperationException(s"Undefined default value for $primaryName")
    }

    def tryGetEnum[EE <: Enumeration](enum: EE)(implicit ev: T <:< String): Try[EE#Value] = {
      tryGet
        .flatMap { v =>
          Try {
            enum.withName(ev(v))
          }
        }
    }
    def getEnum[EE <: Enumeration](enum: EE)(implicit ev: T <:< String) = tryGetEnum(enum).toOption

    def tryGetBoolean(implicit ev: T <:< String): Try[Boolean] = {
      tryGet.map { v =>
        CommonUtils.tryParseBoolean(v).get
      }
    }
    def getBoolean(implicit ev: T <:< String) = tryGetBoolean.toOption

    def tryGetBoolOrInt(implicit ev: T <:< String): Try[Int] = {

      tryGet
        .map(v => ev(v).toInt)
        .recoverWith {
          case _: Exception =>
            tryGetBoolean
              .map {
                case true  => 1
                case false => 0
              }
        }
    }
    def getBoolOrInt(implicit ev: T <:< String) = tryGetBoolOrInt.toOption
  }

  object Attr {

    //TODO: is it useless due to being path dependent?
    implicit def fromStr(v: String): Attr[VV] = new Attr[VV](primaryNameOverride = v)
  }

  type Attr_ = Attr[VV]
}

object EAV extends EAVBuilder[EAV] {

  type Impl = EAVCore
  override def Impl = EAVCore

  trait ImplicitSrc extends EAV

  def empty: Impl = Impl.proto

  trait CaseInsensitive extends EAV {

    override def asMap: Map[String, VV] = asCaseInsensitiveMap
  }
}
