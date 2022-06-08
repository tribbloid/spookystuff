package org.apache.spark.ml.dsl

import org.apache.spark.ml.feature._

/**
  * Created by peng on 27/04/16.
  */
class MapHeadSuite extends AbstractDFDSuite {

  import DFDComponent._

  it("mapHead_> Source doesn't work") {
    intercept[IllegalArgumentException] {
      'input :=>> new Tokenizer() :=>> 'dummy
    }
  }

  it("mapHead_< Source doesn't work") {
    intercept[IllegalArgumentException] {
      'input <<=: new Tokenizer() <<=: 'dummy
    }
  }

  it("mapHead_> can append to 2 heads") {
    val flow = (
      ('input1 U 'input2)
        :=>> new VectorAssembler()
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input1]
        |+- > ForwardNode (HEAD) [input1] > VectorAssembler > [input1$VectorAssembler]
        |> ForwardNode (TAIL>) [input2]
        |+- > ForwardNode (HEAD)(<TAIL) [input2] > VectorAssembler > [input2$VectorAssembler]
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [input2] > VectorAssembler > [input2$VectorAssembler]
      """.stripMargin
      )
  }

  it("mapHead_< can append to 2 heads") {

    val flow = (
      new VectorAssembler()
        <<=: ('input1 U 'input2)
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (HEAD)(TAIL>) [input2] > VectorAssembler > [input2$VectorAssembler]
        |/ right <
        |> ForwardNode (<TAIL) [input1]
        |+- > ForwardNode (HEAD) [input1] > VectorAssembler > [input1$VectorAssembler]
        |> ForwardNode (<TAIL) [input2]
        |+- > ForwardNode (HEAD)(TAIL>) [input2] > VectorAssembler > [input2$VectorAssembler]
      """.stripMargin
      )
  }

  it("mapHead_> can generate 2 stage replicas and append to 2 selected") {

    val flow = (
      (
        'input
          :>> new Tokenizer()
          :>> new StopWordsRemover()
      ).from("Tokenizer")
        .and("StopWordsRemover")
        :=>> new NGram()
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   :- > ForwardNode (HEAD) [input$Tokenizer] > NGram > [input$Tokenizer$NGram]
        |   +- > ForwardNode  [input$Tokenizer] > StopWordsRemover > [input$Tokenizer$StopWordsRemover]
        |      +- > ForwardNode (HEAD)(<TAIL) [input$Tokenizer$StopWordsRemover] > NGram > [input$Tokenizer$StopWordsRemover$NGram]
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [input$Tokenizer$StopWordsRemover] > NGram > [input$Tokenizer$StopWordsRemover$NGram]
      """.stripMargin
      )
  }

  it("mapHead_< can generate 2 stage replicas and append to 2 selected") {

    val flow = (
      new NGram()
        <<=: (
        new StopWordsRemover()
          <<: new Tokenizer()
          <<: 'input
      ).from("Tokenizer")
        .and("StopWordsRemover")
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (HEAD)(TAIL>) [input$Tokenizer$StopWordsRemover] > NGram > [input$Tokenizer$StopWordsRemover$NGram]
        |/ right <
        |> ForwardNode (<TAIL) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   :- > ForwardNode (HEAD) [input$Tokenizer] > NGram > [input$Tokenizer$NGram]
        |   +- > ForwardNode  [input$Tokenizer] > StopWordsRemover > [input$Tokenizer$StopWordsRemover]
        |      +- > ForwardNode (HEAD)(TAIL>) [input$Tokenizer$StopWordsRemover] > NGram > [input$Tokenizer$StopWordsRemover$NGram]
      """.stripMargin
      )
  }

  it("mapHead_> can generate 2 stage replicas and append to 2 heads") {

    val flow = (
      (
        'input
          :>> new Tokenizer()
          :>> new StopWordsRemover()
      ).from("Tokenizer")
        .and("StopWordsRemover")
        :=>> new NGram()
        :=>> new HashingTF()
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   :- > ForwardNode  [input$Tokenizer] > NGram > [input$Tokenizer$NGram]
        |   :  +- > ForwardNode (HEAD) [input$Tokenizer$NGram] > HashingTF > [input$Tokenizer$NGram$HashingTF]
        |   +- > ForwardNode  [input$Tokenizer] > StopWordsRemover > [input$Tokenizer$StopWordsRemover]
        |      +- > ForwardNode  [input$Tokenizer$StopWordsRemover] > NGram > [input$Tokenizer$StopWordsRemover$NGram]
        |         +- > ForwardNode (HEAD)(<TAIL) [input$Tokenizer$StopWordsRemover$NGram] > HashingTF > [input$Tokenizer$StopWordsRemover$NGram$HashingTF]
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [input$Tokenizer$StopWordsRemover$NGram] > HashingTF > [input$Tokenizer$StopWordsRemover$NGram$HashingTF]
      """.stripMargin
      )
  }

  it("mapHead_< can generate 2 stage replicas and append to 2 heads") {

    val flow = (
      new HashingTF()
        <<=: new NGram()
        <<=: (
        new StopWordsRemover()
          <<: new Tokenizer()
          <<: 'input
      ).from("Tokenizer")
        .and("StopWordsRemover")
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (HEAD)(TAIL>) [input$Tokenizer$StopWordsRemover$NGram] > HashingTF > [input$Tokenizer$StopWordsRemover$NGram$HashingTF]
        |/ right <
        |> ForwardNode (<TAIL) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   :- > ForwardNode  [input$Tokenizer] > NGram > [input$Tokenizer$NGram]
        |   :  +- > ForwardNode (HEAD) [input$Tokenizer$NGram] > HashingTF > [input$Tokenizer$NGram$HashingTF]
        |   +- > ForwardNode  [input$Tokenizer] > StopWordsRemover > [input$Tokenizer$StopWordsRemover]
        |      +- > ForwardNode  [input$Tokenizer$StopWordsRemover] > NGram > [input$Tokenizer$StopWordsRemover$NGram]
        |         +- > ForwardNode (HEAD)(TAIL>) [input$Tokenizer$StopWordsRemover$NGram] > HashingTF > [input$Tokenizer$StopWordsRemover$NGram$HashingTF]
      """.stripMargin
      )
  }

  it("mapHead_> won't remove Source of downstream if it's in tails of both side") {

    val dummy: DFDComponent = 'dummy
    val down = dummy :-> new VectorAssembler() <-: dummy

    val flow = (
      'input
        :=>> down
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt, forward = false)
      .treeNodeShouldBe(
        """
        |< BackwardNode (HEAD) [dummy,dummy,input] > VectorAssembler > [VectorAssembler]
        |:- < BackwardNode (<TAIL) [dummy]
        |:- < BackwardNode (<TAIL) [dummy]
        |+- < BackwardNode (TAIL>) [input]
      """.stripMargin
      )
  }

  it("mapHead_< won't remove Source of downstream if it's in tails of both side") {

    val dummy: DFDComponent = 'dummy
    val down = dummy :-> new VectorAssembler() <-: dummy

    val flow =
      down <<=:
        'input

    flow
      .show(showID = false, compactionOpt = compactionOpt, forward = false)
      .treeNodeShouldBe(
        """
        |< BackwardNode (HEAD) [dummy,dummy,input] > VectorAssembler > [VectorAssembler]
        |:- < BackwardNode (TAIL>) [dummy]
        |:- < BackwardNode (TAIL>) [dummy]
        |+- < BackwardNode (<TAIL) [input]
      """.stripMargin
      )
  }
}

class MapHeadSuite_PruneDownPath extends MapHeadSuite with UsePruneDownPath

class MapHeadSuite_PruneDownPathKeepRoot extends MapHeadSuite with UsePruneDownPathKeepRoot
