package com.tribbloids.spookystuff.commons.data

import ai.acyclic.prover.commons.util.Magnet.OptionMagnet
import com.tribbloids.spookystuff.commons.{CommonUtils, TreeThrowable}
import com.tribbloids.spookystuff.relay.RootTagged
import com.tribbloids.spookystuff.relay.xml.Xml

import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.Try

/**
  * entity-(with)-attribute-value
  */
trait EAVLike extends HasEagerInnerObjects with RootTagged with Serializable {

  def internal: collection.Map[String, Any]

  def system: EAVSystem

  override def rootTag: String = Xml.ROOT

  protected def getLookup: Map[String, Any] = internal.toMap
  @transient final lazy val lookup = getLookup

  @transient private lazy val providedHintStr: Option[String] = {
    if (lookup.isEmpty) {
      None
    } else {
      Some(s"only ${KVs.raw.map(_._1).mkString(", ")} are provided")
    }
  }

  def tryGet(k: String, nullable: Boolean = false): Try[Any] = Try {
    val result = lookup.getOrElse(
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

  def get(k: String, nullable: Boolean = false): Option[Any] = tryGet(k, nullable).toOption

  def contains(k: String): Boolean = tryGet(k).isSuccess

  override def toString: String = KVs.raw.mkString("[", ", ", "]")

  /**
    * by default, declared [[Attr]] will be listed in their order of declaration, then non-declared KVs ordered by keys
    */
  @transient object KVs {

    requireInitialised()

    lazy val raw: Seq[(String, Any)] = internal.toSeq.sortBy(_._1)

    lazy val (declared: Vector[(String, Option[Any])], others: Vector[(String, Option[Any])]) = {

      var map = lookup

      val declared = Attr.declared.toVector.map { attr =>
        attr.allNames.foreach { name =>
          map = map.removed(name)
        }
        attr.name -> attr.get
      }

      val others = map.keys.toVector.sorted.map { k =>
        k -> lookup.get(k)
      }

      declared -> others
    }

    lazy val all: Vector[(String, Option[Any])] = declared ++ others

    lazy val defined: Vector[(String, Any)] = {
      all
        .collect {
          case (k, Some(v)) => k -> v
        }
    }
  }

  @transient lazy val asProperties: Properties = {
    val properties = new Properties()

    KVs.defined
      .foreach { v =>
        properties.put(v._1, v._2)
      }

    properties
  }

//  def attr(v: String): Attr[Any] = new Attr[Any](primaryNameOverride = v)

  @transient object Attr {

    lazy val declared: ArrayBuffer[Attr[_]] = ArrayBuffer.empty

    lazy val nameToAttrMap: mutable.Map[String, Attr[_]] = mutable.Map.empty

    def +=(v: Attr[_]): Unit = {
      declared += v
      v.allNames.foreach { name =>
        nameToAttrMap.updateWith(name) {
          case Some(existing) =>
            throw new UnsupportedOperationException(s"Attribute $name is already defined in ${existing}")
          case None =>
            Some(v)
        }
      }

    }
  }

  trait Accessor[T] {

    protected def compute: T
    private lazy val get: T = compute

    def tryGet: Try[T] = Try(get)
  }

  abstract class Attr[T]( // has to be declared in the EAVLike
      // should only be used in setters
      val aliases: List[String] = Nil,
      nullable: Boolean = false,
      default: OptionMagnet[T] = None,
      nameOverride: OptionMagnet[String] = None
  )(
      implicit
      ev: T <:< Any
  ) extends AttrLike[T]
      with EagerInnerObject
      with Accessor[T]
      with Product {

    {
      Attr.declared += this
    }

    override def toString: String = {
      throw new UnsupportedOperationException(
        "Attribute cannot be used as string, please use primaryName or value instead"
      )
    }

    def outer: EAVLike = EAVLike.this

    final def name: String = nameOverride.getOrElse(productPrefix)

    override def compute: T = {

      val getExplicits: Seq[() => T] = allNames.map { name =>
        { () =>
          outer.tryGet(name, nullable).get.asInstanceOf[T]
        }
      }

      val getDefault = { () =>
        default.getOrElse {
          throw new UnsupportedOperationException(s"Undefined default value for $name")
        }
      }

      TreeThrowable
        .|||^(getExplicits :+ getDefault)
        .get

    }

    case class asEnum[EE <: Enumeration](enum: EE)(
        implicit
        ev: T <:< String
    ) extends Accessor[EE#Value] {

      override protected def compute: EE#Value = {

        val v = Attr.this.value

        val result = enum.withName(ev(v))

        result
      }
    }

    case class asBool()(
        implicit
        ev: T <:< String
    ) extends Accessor[Boolean] {

      override protected def compute: Boolean = {

        val v = Attr.this.value

        CommonUtils.tryParseBoolean(v).get
      }
    }

    case class asInt()(
        implicit
        ev: T <:< String
    ) extends Accessor[Int] {

      override protected def compute: Int = {

        val v = Attr.this.value

        ev(v).toInt
      }
    }
  }
}

object EAVLike {

  implicit def toOps[T <: EAVLike](v: T): EAVOps[T] = EAVOps(v)(v.system.asInstanceOf[EAVSystem.Aux[T]])
}
