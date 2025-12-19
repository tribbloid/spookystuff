package com.tribbloids.spookystuff.commons

import ai.acyclic.prover.commons.util.Causes
import com.tribbloids.spookystuff.testutils.BaseSpec

import scala.util.{Failure, Success, Try}

class TreeExceptionSuite extends BaseSpec {

  describe("AllMustSucceed") {

    it("should return all values if all attempts succeed") {
      val attempts: Seq[Try[Int]] = Seq(Success(1), Success(2))
      val result = TreeException.AllMustSucceed(attempts)
      assert(result == Seq(1, 2))
    }

    it("should throw aggregated exception if any attempt fails and pass extra errors") {

      val e1 = new RuntimeException("e1")
      val e2 = new IllegalArgumentException("e2")
      val extraE = new Exception("extra")

      var seen: Seq[Throwable] = Nil
      val aggE = new IllegalStateException("agg")

      def agg(es: Seq[Throwable]): Throwable = {
        seen = es
        aggE
      }

      val attempts: Seq[Try[Int]] = Seq(Failure(e1), Success(1), Failure(e2))
      val thrown = intercept[IllegalStateException] {
        TreeException.AllMustSucceed(attempts, combine = agg, extra = Seq(extraE, null))
      }

      assert(thrown eq aggE)
      assert(!seen.contains(null))
      assert(seen == Seq(extraE, e1, e2))
    }

    it("alias &&& should behave the same") {
      val attempts: Seq[Try[String]] = Seq(Success("a"), Success("b"))
      val result = TreeException.&&&(attempts)
      assert(result == Seq("a", "b"))
    }
  }

  describe("OneMustSucced") {

    it("should return Nil if attempts is empty") {
      val result = TreeException.OneMustSucced[Int](Nil)
      assert(result.isEmpty)
    }

    it("should return all successes if at least one succeeds") {
      val e1 = new RuntimeException("e1")
      val e2 = new RuntimeException("e2")

      val attempts: Seq[Try[Int]] = Seq(Failure(e1), Success(2), Success(3), Failure(e2))
      val result = TreeException.OneMustSucced(attempts)
      assert(result == Seq(2, 3))
    }

    it("should throw aggregated exception if none succeeds and pass extra errors") {

      val e1 = new RuntimeException("e1")
      val e2 = new IllegalArgumentException("e2")
      val extraE = new Exception("extra")

      var seen: Seq[Throwable] = Nil
      val aggE = new IllegalStateException("agg")

      def agg(es: Seq[Throwable]): Throwable = {
        seen = es
        aggE
      }

      val attempts: Seq[Try[Int]] = Seq(Failure(e1), Failure(e2))
      val thrown = intercept[IllegalStateException] {
        TreeException.OneMustSucced(attempts, agg = agg, extra = Seq(extraE, null))
      }

      assert(thrown eq aggE)
      assert(!seen.contains(null))
      assert(seen == Seq(extraE, e1, e2))
    }

    it("alias ||| should behave the same") {
      val attempts: Seq[Try[Int]] = Seq(Failure(new RuntimeException("e")), Success(1))
      val result = TreeException.|||(attempts)
      assert(result == Seq(1))
    }
  }

  describe("GetFirstSuccess") {

    it("should return None if attempts is empty") {
      val result = TreeException.GetFirstSuccess[Int](Nil)
      assert(result.isEmpty)
    }

    it("should early-exit on first success") {

      var called: Vector[String] = Vector.empty

      val f1: () => Int = { () =>
        called = called :+ "1"
        throw new RuntimeException("e1")
      }

      val f2: () => Int = { () =>
        called = called :+ "2"
        7
      }

      val f3: () => Int = { () =>
        called = called :+ "3"
        9
      }

      val result = TreeException.GetFirstSuccess(Seq(f1, f2, f3))
      assert(result.contains(7))
      assert(called == Vector("1", "2"))
    }

    it("should throw aggregated exception if all fail and pass extra errors") {

      val e1 = new RuntimeException("e1")
      val e2 = new IllegalArgumentException("e2")
      val extraE = new Exception("extra")

      var seen: Seq[Throwable] = Nil
      val aggE = new IllegalStateException("agg")

      def agg(es: Seq[Throwable]): Throwable = {
        seen = es
        aggE
      }

      val f1: () => Int = { () => throw e1 }
      val f2: () => Int = { () => throw e2 }

      val thrown = intercept[IllegalStateException] {
        TreeException.GetFirstSuccess(Seq(f1, f2), agg = agg, extra = Seq(extraE, null))
      }

      assert(thrown eq aggE)
      assert(!seen.contains(null))
      assert(seen == Seq(extraE, e1, e2))
    }

    it("alias |||^ should behave the same") {
      val attempts: Seq[() => Int] = Seq(() => 1)
      val result = TreeException.|||^(attempts)
      assert(result.contains(1))
    }

    it("should handle AssertionError and still return first success") {

      var called: Vector[String] = Vector.empty

      val f1: () => Int = { () =>
        called = called :+ "1"
        assert(false)
        1
      }

      val f2: () => Int = { () =>
        called = called :+ "2"
        11
      }

      val f3: () => Int = { () =>
        called = called :+ "3"
        22
      }

      val result = TreeException.GetFirstSuccess(Seq(f1, f2, f3))
      assert(result.contains(11))
      assert(called == Vector("1", "2"))
    }
  }

  describe("_TreeView") {

    case class MyTreeException(
        msg: String,
        _causes: Seq[Throwable]
    ) extends Exception(msg)
        with TreeException {

      override def getCause: Throwable = Causes(_causes)

      override def getMessage_simple: String = msg
    }

    it("should list causes for HasCauses and sort children by nodeText") {

      val a = new RuntimeException("a")
      val b = new RuntimeException("b")

      val parent: Throwable = Causes(Seq[Throwable](b, a))
      val tv = TreeException._TreeView(parent)

      val childrenText = tv.children.map(_.nodeText)
      assert(childrenText == Seq("java.lang.RuntimeException: a", "java.lang.RuntimeException: b"))
    }

    it("should fall back to getCause for non-HasCauses") {

      val inner = new RuntimeException("inner")
      val outer = new RuntimeException("outer", inner)

      val tv = TreeException._TreeView(outer)
      assert(tv.children.map(_.nodeText) == Seq("java.lang.RuntimeException: inner"))
    }

    it("should format TreeException nodeText using getMessage_simple") {

      val e = MyTreeException("AA\r\nBB", Nil)
      val tv = TreeException._TreeView(e)

      assert(tv.nodeText == "AA\nBB")
      assert(!tv.nodeText.contains('\r'))
    }
  }
}
