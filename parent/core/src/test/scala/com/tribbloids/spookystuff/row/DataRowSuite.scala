package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.testutils.SpookyBaseSpec

/**
  * Created by peng on 30/03/16.
  */
class DataRowSuite extends SpookyBaseSpec {

  import com.tribbloids.spookystuff.dsl._

  // TODO: cleanup, pattern-based interpolation is prone to error
//  it("interpolate") {
//    val map = Map(Alias("abc") -> 1, Alias("def") -> 2.2)
//    val result = DataRow(map).replaceInto("rpk'{abc}aek'{def}")
//    assert(result === Some("rpk1aek2.2"))
//  }
//
//  it("interpolate returns None when key not found") {
//
//    val map = Map(Alias("abc") -> 1, Alias("rpk") -> 2.2)
//    val result = DataRow(map).replaceInto("rpk'{abc}aek'{def}")
//    assert(result === None)
//  }

  it("formatNullString") {
    assert(DataRow(Map()).replaceInto(null).isEmpty)
  }

  it("formatEmptyString") {
    assert(DataRow(Map()).replaceInto("").contains(""))
  }

  it("getInt can extract scala Int type") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Array("d", "e", "f"))
    val result = DataRow(map).getInt('abc).get
    assert(result == 1)
  }

  it("getInt can extract java.lang.Integer type") {
    val map: Map[Alias, Any] = Map(Alias("abc") -> (1: java.lang.Integer), Alias("def") -> Array("d", "e", "f"))
    val result = DataRow(map).getInt('abc).get
    assert(result == 1)
  }

  it("getTyped can extract scala Int type") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Array("d", "e", "f"))
    val result = DataRow(map).getTyped[Int]('abc).get
    assert(result == 1)
  }

  it("getTyped can extract java.lang.Integer type") {
    val map: Map[Alias, Any] = Map(Alias("abc") -> (1: java.lang.Integer), Alias("def") -> Array("d", "e", "f"))
    val result = DataRow(map).getTyped[Int]('abc).get
    assert(result == 1)
  }

  it("getTyped should return None if type is incompatible") {
    val map: Map[Alias, Any] = Map(Alias("abc") -> (1.1: java.lang.Double), Alias("def") -> Array("d", "e", "f"))
    val result = DataRow(map).getTyped[Int]('abc)
    assert(result.isEmpty)
  }

  it("getTypedArray can extract from Array") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Array("d", "e", "f"))
    val result = DataRow(map).getTypedArray[String]('def).get
    assert(result.toSeq == Seq("d", "e", "f"))
  }

  it("getTypedArray can extract from Iterator") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Seq("d", "e", "f").iterator)
    val result = DataRow(map).getTypedArray[String]('def).get
    assert(result.toSeq == Seq("d", "e", "f"))
  }

  it("getTypedArray can extract from Set") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Set("d", "e", "f"))
    val result = DataRow(map).getTypedArray[String]('def).get
    assert(result.toSet == Set("d", "e", "f"))
  }

  it("getTypedArray can extract from Array that has different types") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Set(2, 3.3, "def"))
    val result = DataRow(map).getTypedArray[String]('def).get
    assert(result.toSet == Set("def"))
  }

  it("getIntArray can extract from Array") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Array[Int](2, 3, 4))
    val result = DataRow(map).getIntArray('def).get
    assert(result.toSeq == Seq(2, 3, 4))
  }

  it("getIntArray can extract from Iterator") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Seq[Int](2, 3, 4).iterator)
    val result = DataRow(map).getIntArray('def).get
    assert(result.toSeq == Seq(2, 3, 4))
  }

  it("getIntArray can extract from Set") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Set[Int](2, 3, 4))
    val result = DataRow(map).getIntArray('def).get
    assert(result.toSet == Set(2, 3, 4))
  }

  it("getIntArray can extract from Array that has different types") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Set(2, 3.3, "def"))
    val result = DataRow(map).getIntArray('def).get
    assert(result.toSet == Set(2))
  }

  it("getIntIterable can extract from Array") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Array[Int](2, 3, 4))
    val result = DataRow(map).getIntIterable('def).get
    assert(result.toSeq == Seq(2, 3, 4))
  }

  it("getIntIterable can extract from Iterator") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Seq[Int](2, 3, 4).iterator)
    val result = DataRow(map).getIntIterable('def).get
    assert(result.toSeq == Seq(2, 3, 4))
  }

  it("getIntIterable can extract from Set") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Set[Int](2, 3, 4))
    val result = DataRow(map).getIntIterable('def).get
    assert(result.toSet == Set(2, 3, 4))
  }

  it("getIntIterable can extract from Array that has different types") {
    val map = Map(Alias("abc") -> 1, Alias("def") -> Set(2, 3.3, "def"))
    val result = DataRow(map).getIntIterable('def).get
    assert(result.toSet == Set(2))
  }
}
