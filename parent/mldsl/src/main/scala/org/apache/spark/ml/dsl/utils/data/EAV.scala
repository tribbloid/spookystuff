package org.apache.spark.ml.dsl.utils.data

import com.tribbloids.spookystuff.utils.{CommonUtils, EqualBy, TreeThrowable}
import org.apache.spark.ml.dsl.utils.messaging.RootTagged
import org.apache.spark.ml.dsl.utils.messaging.xml.Xml
import org.apache.spark.ml.dsl.utils.{?, HasEagerInnerObjects, ObjectSimpleNameMixin}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.util.Properties
import scala.collection.MapView
import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.util.Try

/**
  * entity-(with)-attribute-value
  */
trait EAV extends HasEagerInnerObjects with EqualBy with RootTagged with Serializable {

  type Bound

  def internal: collection.Map[String, Bound]

  def system: EAVSystem

  override lazy val rootTag: String = Xml.ROOT

  private lazy val _asMap: ListMap[String, Bound] = {

    val keys = internal.keys.toSeq
    val kvs = keys.sorted.map { k =>
      k -> internal(k)
    }
    ListMap(kvs: _*)
  }
  lazy val asCaseInsensitiveMap: CaseInsensitiveMap[Bound] = CaseInsensitiveMap(_asMap)

  def asMap: Map[String, Bound] = _asMap
  lazy val asMapOfString: MapView[String, String] = asMap.view.mapValues(v => "" + v)

  // TODO: change to declaredAttrs?
  override def _equalBy: Any = asMap

  lazy val asProperties: Properties = {
    val properties = new Properties()

    asMapOfString.foreach { v =>
      properties.put(v._1, v._2)
    }

    properties
  }

  override def toString: String = asMapOfString.mkString("[", ", ", "]")

  lazy val providedHintStr: Option[String] = {
    if (asMapOfString.isEmpty) {
      None
    } else {
      Some(s"only ${asMap.keys.mkString(", ")} are provided")
    }
  }

  // TODO: cleanup for being too redundant! not encouraged to use

  def tryGet(k: String, nullable: Boolean = false): Try[Bound] = Try {
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

  def get(k: String, nullable: Boolean = false): Option[Bound] = tryGet(k, nullable).toOption

  def apply(k: String, nullable: Boolean = false): Bound = tryGet(k, nullable).get

  def getOrElse(k: String, default: Bound): Bound = {
    require(
      default != null,
      s"default value for `$k` cannot be null"
    )
    get(k).getOrElse(default)
  }

  def contains(k: String): Boolean = tryGet(k).isSuccess

  def attr(v: String): Attr[Bound] = new Attr_(primaryNameOverride = v)

  trait _AttrLike[T] extends AttrLike[T] with EagerInnerObject with ObjectSimpleNameMixin

  class Attr[T](
      // should only be used in setters
      val aliases: List[String] = Nil,
      nullable: Boolean = false,
      default: T `?` _ = None,
      primaryNameOverride: String `?` _ = None
  )(
      implicit
      ev: T <:< Bound
  ) extends _AttrLike[T] {

    override def toString: String = {
      throw new UnsupportedOperationException(
        "Attribute cannot be used as string, please use primaryName or value instead"
      )
    }

    def outer: EAV = EAV.this

    final def primaryName: String = primaryNameOverride.getOrElse(objectSimpleName)

    def explicitValue: T = {
      val trials: Seq[() => T] = allNames.map {
        name =>
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

    def tryGetEnum[EE <: Enumeration](enum: EE)(
        implicit
        ev: T <:< String
    ): Try[EE#Value] = {
      tryGet
        .flatMap { v =>
          Try {
            enum.withName(ev(v))
          }
        }
    }

    def tryGetBoolean(
        implicit
        ev: T <:< String
    ): Try[Boolean] = {
      tryGet.map { v =>
        CommonUtils.tryParseBoolean(v).get
      }
    }

    def tryGetBoolOrInt(
        implicit
        ev: T <:< String
    ): Try[Int] = {

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
  }

  object Attr {

    // TODO: is it useless due to being path dependent?
    implicit def fromStr(v: String): Attr[Bound] = new Attr[Bound](primaryNameOverride = v)
  }

  type Attr_ = Attr[Bound]
}

object EAV {

  trait CaseInsensitive extends EAV {

    override def asMap: Map[String, Bound] = asCaseInsensitiveMap
  }

  implicit def toOps[T <: EAV](v: T): EAVOps[T] = EAVOps(v)(v.system.asInstanceOf[EAVSystem.Aux[T]])
}
