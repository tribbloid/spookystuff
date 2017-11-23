package com.tribbloids.spookystuff.extractors.impl

import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.extractors.GenExtractor.AndThen
import com.tribbloids.spookystuff.extractors._
import com.tribbloids.spookystuff.row._

object Extractors {

  val GroupIndexExpr = GenExtractor.fromFn{
    (v1: FR) => v1.dataRow.groupIndex
  }

  //
  def GetUnstructuredExpr(field: Field): GenExtractor[FR, Unstructured] = GenExtractor.fromOptionFn {
    (v1: FR) =>
      v1.getUnstructured(field)
        .orElse{
          v1.getUnstructured(field.copy(isWeak = true))
        }
        .orElse {
          v1.getDoc(field.name).map(_.root)
        }
  }

  def GetDocExpr(field: Field) = GenExtractor.fromOptionFn {
    (v1: FR) => v1.getDoc(field.name)
  }
  val GetOnlyDocExpr = GenExtractor.fromOptionFn {
    (v1: FR) => v1.getOnlyDoc
  }
  val GetAllDocsExpr = GenExtractor.fromFn {
    (v1: FR) => new Elements(v1.docs.map(_.root).toList)
  }

  case class FindAllMeta(arg: Extractor[Unstructured], selector: String)
  def FindAllExpr(arg: Extractor[Unstructured], selector: String) = arg.andFn(
    {
      v1: Unstructured => v1.findAll(selector)
    },
    Some(FindAllMeta(arg, selector))
  )

  case class ChildrenMeta(arg: Extractor[Unstructured], selector: String)
  def ChildrenExpr(arg: Extractor[Unstructured], selector: String) = arg.andFn(
    {
      v1 => v1.children(selector)
    },
    Some(ChildrenMeta(arg, selector))
  )

  def ExpandExpr(arg: Extractor[Unstructured], range: Range) = {
    arg match {
      case AndThen(_,_,Some(FindAllMeta(argg, selector))) =>
        argg.andFn(_.findAllWithSiblings(selector, range))
      case AndThen(_,_,Some(ChildrenMeta(argg, selector))) =>
        argg.andFn(_.childrenWithSiblings(selector, range))
      case _ =>
        throw new UnsupportedOperationException("expression does not support expand")
    }
  }

  def ReplaceKeyExpr(str: String) = GenExtractor.fromOptionFn {
    (v1: FR) =>
      v1.dataRow.replaceInto(str)
  }
}













