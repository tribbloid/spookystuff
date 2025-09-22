//package com.tribbloids.spookystuff.row
//
//import com.tribbloids.spookystuff.testutils.SpookyBaseSpec
//
///**
//  * Created by peng on 30/03/16.
//  */
//class DataSuite extends SpookyBaseSpec {
//
//  import com.tribbloids.spookystuff.dsl._
//
//  it("getInt can extract scala Int type") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Array("d", "e", "f"))
//    val result = Lineage(map).getInt('abc).get
//    assert(result == 1)
//  }
//
//  it("getInt can extract java.lang.Integer type") {
//    val map: Map[Field, Any] = Map(Field("abc") -> (1: java.lang.Integer), Field("def") -> Array("d", "e", "f"))
//    val result = Lineage(map).getInt('abc).get
//    assert(result == 1)
//  }
//
//  it("getTyped can extract scala Int type") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Array("d", "e", "f"))
//    val result = Lineage(map).getTyped[Int]('abc).get
//    assert(result == 1)
//  }
//
//  it("getTyped can extract java.lang.Integer type") {
//    val map: Map[Field, Any] = Map(Field("abc") -> (1: java.lang.Integer), Field("def") -> Array("d", "e", "f"))
//    val result = Lineage(map).getTyped[Int]('abc).get
//    assert(result == 1)
//  }
//
//  it("getTyped should return None if type is incompatible") {
//    val map: Map[Field, Any] = Map(Field("abc") -> (1.1: java.lang.Double), Field("def") -> Array("d", "e", "f"))
//    val result = Lineage(map).getTyped[Int]('abc)
//    assert(result.isEmpty)
//  }
//
//  it("getTypedArray can extract from Array") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Array("d", "e", "f"))
//    val result = Lineage(map).getArray[String]('def).get
//    assert(result.toSeq == Seq("d", "e", "f"))
//  }
//
//  it("getTypedArray can extract from Iterator") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Seq("d", "e", "f").iterator)
//    val result = Lineage(map).getArray[String]('def).get
//    assert(result.toSeq == Seq("d", "e", "f"))
//  }
//
//  it("getTypedArray can extract from Set") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Set("d", "e", "f"))
//    val result = Lineage(map).getArray[String]('def).get
//    assert(result.toSet == Set("d", "e", "f"))
//  }
//
//  it("getTypedArray can extract from Array that has different types") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Set(2, 3.3, "def"))
//    val result = Lineage(map).getArray[String]('def).get
//    assert(result.toSet == Set("def"))
//  }
//
//  it("getIntArray can extract from Array") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Array[Int](2, 3, 4))
//    val result = Lineage(map).getArray[Int]('def).get
//    assert(result.toSeq == Seq(2, 3, 4))
//  }
//
//  it("getIntArray can extract from Iterator") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Seq[Int](2, 3, 4).iterator)
//    val result = Lineage(map).getArray[Int]('def).get
//    assert(result.toSeq == Seq(2, 3, 4))
//  }
//
//  it("getIntArray can extract from Set") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Set[Int](2, 3, 4))
//    val result = Lineage(map).getArray[Int]('def).get
//    assert(result.toSet == Set(2, 3, 4))
//  }
//
//  it("getIntArray can extract from Array that has different types") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Set(2, 3.3, "def"))
//    val result = Lineage(map).getArray[Int]('def).get
//    assert(result.toSet == Set(2))
//  }
//
//  it("getIntIterable can extract from Array") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Array[Int](2, 3, 4))
//    val result = Lineage(map).getIntIterable('def).get
//    assert(result.toSeq == Seq(2, 3, 4))
//  }
//
//  it("getIntIterable can extract from Iterator") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Seq[Int](2, 3, 4).iterator)
//    val result = Lineage(map).getIntIterable('def).get
//    assert(result.toSeq == Seq(2, 3, 4))
//  }
//
//  it("getIntIterable can extract from Set") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Set[Int](2, 3, 4))
//    val result = Lineage(map).getIntIterable('def).get
//    assert(result.toSet == Set(2, 3, 4))
//  }
//
//  it("getIntIterable can extract from Array that has different types") {
//    val map = Map(Field("abc") -> 1, Field("def") -> Set(2, 3.3, "def"))
//    val result = Lineage(map).getIntIterable('def).get
//    assert(result.toSet == Set(2))
//  }
//}
