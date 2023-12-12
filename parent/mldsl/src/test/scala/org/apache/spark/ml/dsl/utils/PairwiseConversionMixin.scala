package org.apache.spark.ml.dsl.utils

import com.tribbloids.spookystuff.testutils.BaseSpec
import com.tribbloids.spookystuff.utils.MultiMapView

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait PairwiseConversionMixin extends BaseSpec {

  import PairwiseConversionMixin._

  val registryKeys: ArrayBuffer[String] = ArrayBuffer.empty[String]
  val registry: MultiMapView.Mutable[String, () => Unit] =
    MultiMapView.Mutable.empty

  trait PairwiseCases extends Serializable {

    def cases: Seq[PairwiseCase[_, _]]

    def registerAll(): Unit = {

      cases.foreach(_.register())
    }
  }

  case class PairwiseCase[T1: ClassTag, T2: ClassTag](
      from: Repr[T1],
      to: Repr[T2],
      forward: T1 => T2,
      backward: T2 => T1
  ) extends PairwiseCases {

    def register(): Unit = {
      from match {
        case Repr(Some(r1), l1) =>
          val description = s"[${from.valueStr} <: ${from.classStr}]"
          registryKeys += description
          registry.put1(
            description,
            { () =>
              to match {
                case Repr(_, l2) if l2 >= 0 && l2 == l1 =>
                  it(s"<==> ${to.classStr}") {

                    val _r2 = forward(r1)
                    to.assertForallEquals(_r2)

                    val _r1 = backward(_r2)
                    from.assertForallEquals(_r1)
                  }

                case Repr(_, l2) if l2 > 0 && l2 < l1 =>
                  it(s"==> ${to.classStr} ==> ?") {

                    val _r2 = forward(r1)
                    to.assertForallEquals(_r2)

                    backward(_r2)
                  }

                case Repr(_, l2) if l2 == 0 && l2 < l1 =>
                  it(s"==> ${to.classStr}") {

                    val _r2 = forward(r1)
                    to.assertForallEquals(_r2)
                  }

                case _ =>
              }
            }
          )
        case _ =>
      }
    }

    override lazy val cases: Seq[PairwiseCase[_, _]] = Seq(this)

    lazy val inverse: PairwiseCase[T2, T1] = PairwiseCase(to, from, backward, forward)
    lazy val bidirCases: Seq[PairwiseCase[_, _]] = Seq(this, this.inverse)
  }

  def runAll(cases: PairwiseCases*): Unit = {

    cases.foreach { v =>
      v.registerAll()
    }

    registryKeys.distinct.foreach { k =>
      val fns = registry(k)
      describe(k) {

        fns.foreach(_.apply())
      }
    }
  }
}

object PairwiseConversionMixin {

  case class Repr[T: ClassTag](
      opt: Option[_ <: T],
      level: Int = 10
      // repr -> level, < 0 means disabled
      // can only convert to Repr with lower level
  ) {

    def classStr: String = implicitly[ClassTag[T]].runtimeClass.getSimpleName

    lazy val valueStr: String = s"${opt.getOrElse("∅")}"

    override lazy val toString: String = opt.map(_.toString).getOrElse("?")

    def map[R: ClassTag](fn: T => R): Repr[R] = Repr[R](
      opt.map(fn),
      level
    )

    def forallEquals(v2: T): Boolean = {

      this.opt match {
        case Some(v1) =>
          v1 == v2
        case _ =>
          true
      }
    }

    final def assertForallEquals(v2: T): Unit = {

      val result = forallEquals(v2)

      Predef.assert(result, s"${this.opt.get} != $v2")
    }
  }

  object Repr {

    def disabled[T: ClassTag]: Repr[T] = Repr[T](None, -1)

    def unvalidated_<=>[T: ClassTag]: Repr[T] = Repr[T](None, 1)

    def unvalidated_=>[T: ClassTag]: Repr[T] = Repr[T](None, 0)
  }
}
