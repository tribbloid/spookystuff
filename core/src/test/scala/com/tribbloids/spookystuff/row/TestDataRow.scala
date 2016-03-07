package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyEnvSuite

/**
  * Created by peng on 30/03/16.
  */
class TestDataRow extends SpookyEnvSuite {

  import com.tribbloids.spookystuff.dsl._

  test("interpolate") {
    val map = Map(Field("abc") -> 1, Field("def") -> 2.2)
    val result = DataRow(map).replaceInto("rpk'{abc}aek'{def}")
    assert(result === Some("rpk1aek2.2"))
  }

  test("interpolate returns None when key not found") {

    val map = Map(Field("abc") -> 1, Field("rpk") -> 2.2)
    val result = DataRow(map).replaceInto("rpk'{abc}aek'{def}")
    assert(result === None)
  }

  test("formatNullString") {
    assert(DataRow(Map()).replaceInto(null).isEmpty)
  }

  test("formatEmptyString") {
    assert(DataRow(Map()).replaceInto("").exists(_ == ""))
  }

  test("getTypedArray can extract from Array") {
    val map = Map(Field("abc") -> 1, Field("def") -> Array("d","e", "f"))
    val result = DataRow(map).getTypedArray[String]('def).get
    assert(result.toSeq == Seq("d","e", "f"))
  }

  test("getTypedArray can extract from Iterator") {
    val map = Map(Field("abc") -> 1, Field("def") -> Seq("d","e", "f").iterator)
    val result = DataRow(map).getTypedArray[String]('def).get
    assert(result.toSeq == Seq("d","e", "f"))
  }

  test("getTypedArray can extract from Set") {
    val map = Map(Field("abc") -> 1, Field("def") -> Set("d","e", "f"))
    val result = DataRow(map).getTypedArray[String]('def).get
    assert(result.toSet == Set("d","e", "f"))
  }

  test("getTypedArray can extract from Array that has different types") {
    val map = Map(Field("abc") -> 1, Field("def") -> Set(2,3.3,"def"))
    val result = DataRow(map).getTypedArray[String]('def).get
    assert(result.toSet == Set("def"))
  }



  test("getIntArray can extract from Array") {
    val map = Map(Field("abc") -> 1, Field("def") -> Array[Int](2,3,4))
    val result = DataRow(map).getIntArray('def).get
    assert(result.toSeq == Seq(2,3,4))
  }

  test("getIntArray can extract from Iterator") {
    val map = Map(Field("abc") -> 1, Field("def") -> Seq[Int](2,3,4).iterator)
    val result = DataRow(map).getIntArray('def).get
    assert(result.toSeq == Seq(2,3,4))
  }

  test("getIntArray can extract from Set") {
    val map = Map(Field("abc") -> 1, Field("def") -> Set[Int](2,3,4))
    val result = DataRow(map).getIntArray('def).get
    assert(result.toSet == Set(2,3,4))
  }

  test("getIntArray can extract from Array that has different types") {
    val map = Map(Field("abc") -> 1, Field("def") -> Set(2,3.3,"def"))
    val result = DataRow(map).getIntArray('def).get
    assert(result.toSet == Set(2))
  }

  
  
  test("getIntIterable can extract from Array") {
    val map = Map(Field("abc") -> 1, Field("def") -> Array[Int](2,3,4))
    val result = DataRow(map).getIntIterable('def).get
    assert(result.toSeq == Seq(2,3,4))
  }

  test("getIntIterable can extract from Iterator") {
    val map = Map(Field("abc") -> 1, Field("def") -> Seq[Int](2,3,4).iterator)
    val result = DataRow(map).getIntIterable('def).get
    assert(result.toSeq == Seq(2,3,4))
  }

  test("getIntIterable can extract from Set") {
    val map = Map(Field("abc") -> 1, Field("def") -> Set[Int](2,3,4))
    val result = DataRow(map).getIntIterable('def).get
    assert(result.toSet == Set(2,3,4))
  }

  test("getIntIterable can extract from Array that has different types") {
    val map = Map(Field("abc") -> 1, Field("def") -> Set(2,3.3,"def"))
    val result = DataRow(map).getIntIterable('def).get
    assert(result.toSet == Set(2))
  }
}
