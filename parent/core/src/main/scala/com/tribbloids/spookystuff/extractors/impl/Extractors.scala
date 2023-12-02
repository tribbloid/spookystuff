package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.row._

object Extractors {

  val GroupIndexExpr: GenExtractor[FR, Int] = GenExtractor.fromFn { v1: FR =>
    v1.trajectory.ordinal
  }

  //
  def GetUnstructuredExpr(field: Field): GenExtractor[FR, Unstructured] = GenExtractor.fromOptionFn { v1: FR =>
    v1.getUnstructured(field)
      .orElse {
        v1.getUnstructured(field.copy(isWeak = true))
      }
      .orElse {
        v1.getDoc(field.name).map(_.root)
      }
  }

  def GetDocExpr(field: Field): GenExtractor[FR, Doc] = GenExtractor.fromOptionFn { v1: FR =>
    v1.getDoc(field.name)
  }
  val GetOnlyDocExpr: GenExtractor[FR, Doc] = GenExtractor.fromOptionFn { v1: FR =>
    v1.getOnlyDoc
  }
  val GetAllDocsExpr: GenExtractor[FR, Elements[Unstructured]] = GenExtractor.fromFn { v1: FR =>
    new Elements(v1.docs.map(_.root).toList)
  }

  case class FindAllMeta(arg: Extractor[Unstructured], selector: String)
  def FindAllExpr(arg: Extractor[Unstructured], selector: String): GenExtractor[FR, Elements[Unstructured]] =
    arg.andMap(
      { v1: Unstructured =>
        v1.findAll(selector)
      },
      Some(FindAllMeta(arg, selector))
    )

  case class ChildrenMeta(arg: Extractor[Unstructured], selector: String)
  def ChildrenExpr(arg: Extractor[Unstructured], selector: String): GenExtractor[FR, Elements[Unstructured]] =
    arg.andMap(
      { v1 =>
        v1.children(selector)
      },
      Some(ChildrenMeta(arg, selector))
    )
}
