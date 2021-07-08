package org.apache.spark.ml.dsl

/**
  * Created by peng on 23/04/16.
  */
class TrieNodeSuite extends AbstractDFDSuite {

  it("compact can merge single child parents") {
    val map = Seq(
      "A",
      "AB",
      "ABC",
      "ABCD",
      "ABCDE",
      "ABCDF",
      "1",
      "12",
      "123",
      "124"
    ).map(_.split("").toSeq)
      .map(v => v -> v.mkString)

    val trie = TrieNode.build(map)

    trie
      .toString()
      .treeNodeShouldBe(
        """
        |TrieNode 0
        |:- TrieNode [A], A, 1
        |:  +- TrieNode [A, B], AB, 2
        |:     +- TrieNode [A, B, C], ABC, 3
        |:        +- TrieNode [A, B, C, D], ABCD, 4
        |:           :- TrieNode [A, B, C, D, E], ABCDE, 5
        |:           +- TrieNode [A, B, C, D, F], ABCDF, 5
        |+- TrieNode [1], 1, 1
        |   +- TrieNode [1, 2], 12, 2
        |      :- TrieNode [1, 2, 3], 123, 3
        |      +- TrieNode [1, 2, 4], 124, 3
      """.stripMargin
      )

    trie.compact
      .rebuildDepth()
      .toString()
      .treeNodeShouldBe(
        """
        |TrieNode 0
        |:- TrieNode [A, B, C, D], ABCD, 1
        |:  :- TrieNode [A, B, C, D, E], ABCDE, 2
        |:  +- TrieNode [A, B, C, D, F], ABCDF, 2
        |+- TrieNode [1, 2], 12, 1
        |   :- TrieNode [1, 2, 3], 123, 2
        |   +- TrieNode [1, 2, 4], 124, 2
      """.stripMargin
      )
  }

  it("pruneUp can rename single children") {
    val map = Seq(
      "A",
      "AB",
      "ABC",
      "ABCD",
      "ABCDE",
      "ABCDF",
      "1",
      "12",
      "123",
      "124"
    ).map(_.split("").toSeq)
      .map(v => v -> v.mkString)

    val trie = TrieNode.build(map)

    trie
      .toString()
      .treeNodeShouldBe(
        """
        |TrieNode 0
        |:- TrieNode [A], A, 1
        |:  +- TrieNode [A, B], AB, 2
        |:     +- TrieNode [A, B, C], ABC, 3
        |:        +- TrieNode [A, B, C, D], ABCD, 4
        |:           :- TrieNode [A, B, C, D, E], ABCDE, 5
        |:           +- TrieNode [A, B, C, D, F], ABCDF, 5
        |+- TrieNode [1], 1, 1
        |   +- TrieNode [1, 2], 12, 2
        |      :- TrieNode [1, 2, 3], 123, 3
        |      +- TrieNode [1, 2, 4], 124, 3
      """.stripMargin
      )

    trie.pruneUp
      .rebuildDepth()
      .toString()
      .treeNodeShouldBe(
        """
        |TrieNode 0
        |:- TrieNode [A], A, 1
        |:  +- TrieNode [A], AB, 2
        |:     +- TrieNode [A], ABC, 3
        |:        +- TrieNode [A], ABCD, 4
        |:           :- TrieNode [A, E], ABCDE, 5
        |:           +- TrieNode [A, F], ABCDF, 5
        |+- TrieNode [1], 1, 1
        |   +- TrieNode [1], 12, 2
        |      :- TrieNode [1, 3], 123, 3
        |      +- TrieNode [1, 4], 124, 3
      """.stripMargin
      )
  }

  it("reversed pruneUp can minimize names") {
    val names =
      """
        |AB
        |ABC
        |ABCD
        |ABCDE
        |ABK
        |ABCK
        |ABCDK
        |ABCDEK
      """.trim.stripMargin
        .split("\n")
        .map(_.split("").toSeq)

    val trie = TrieNode.build(
      names
        .map(_.reverse)
        .map(v => v -> v)
    )

    val pairs = trie.pruneUp
      .flatMap { node =>
        val k = node.key
        node.value.map(_ -> k)
      }
      .map(tuple => tuple._1.reverse -> tuple._2.reverse)

    val map = Map(pairs: _*)
    val result = names.map { v =>
      v.mkString -> map(v).mkString
    }
    result
      .mkString("\n")
      .shouldBe(
        """
        |(AB,B)
        |(ABC,C)
        |(ABCD,D)
        |(ABCDE,E)
        |(ABK,BK)
        |(ABCK,CK)
        |(ABCDK,DK)
        |(ABCDEK,EK)
      """.stripMargin
      )
  }

  it("reversed compact can minimize repeated names") {
    val names =
      """
        |A
        |AA
        |AAA
        |AAAA
        |AAAAA
        |AAAAAA
        |AAAAAB
      """.trim.stripMargin
        .split("\n")
        .map(_.split("").toSeq)

    val trie = TrieNode.build(
      names
        .map(_.reverse)
        .map(v => v -> v)
    )

    val pairs = trie.pruneUp
      .flatMap { node =>
        val k = node.key
        node.value.map(_ -> k)
      }
      .map(tuple => tuple._1.reverse -> tuple._2.reverse)

    val map = Map(pairs: _*)
    val result = names.map { v =>
      v.mkString -> map(v).mkString
    }
    result
      .mkString("\n")
      .shouldBe(
        """
        |(A,A)
        |(AA,A)
        |(AAA,A)
        |(AAAA,A)
        |(AAAAA,A)
        |(AAAAAA,A)
        |(AAAAAB,B)
      """.stripMargin
      )
  }

  it("reversed compact can minimize some names") {
    val names =
      """
        |input Tokenizer
        |input Tokenizer HashingTF
        |input Tokenizer StopWordsRemover HashingTF
        |input Tokenizer HashingTF VectorAssembler
        |input Tokenizer StopWordsRemover HashingTF VectorAssembler
        |input
        |input Tokenizer StopWordsRemover
      """.trim.stripMargin
        .split("\n")
        .map(_.split(" ").toSeq)

    val trie = TrieNode.build(
      names
        .map(_.reverse)
        .map(v => v -> v)
    )

    val pairs = trie.pruneUp
      .flatMap { node =>
        val k = node.key
        node.value.map(_ -> k)
      }
      .map(tuple => tuple._1.reverse -> tuple._2.reverse)

    val map = Map(pairs: _*)
    val result = names.map { v =>
      v.mkString(" ") -> map(v).mkString(" ")
    }
    result
      .mkString("\n")
      .shouldBe(
        """
        |(input Tokenizer,Tokenizer)
        |(input Tokenizer HashingTF,Tokenizer HashingTF)
        |(input Tokenizer StopWordsRemover HashingTF,StopWordsRemover HashingTF)
        |(input Tokenizer HashingTF VectorAssembler,Tokenizer VectorAssembler)
        |(input Tokenizer StopWordsRemover HashingTF VectorAssembler,StopWordsRemover VectorAssembler)
        |(input,input)
        |(input Tokenizer StopWordsRemover,StopWordsRemover)
      """.stripMargin
      )
  }
}
