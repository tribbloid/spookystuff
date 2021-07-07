package org.apache.spark.ml.dsl

import org.apache.spark.ml.feature._

/**
  * Created by peng on 27/04/16.
  */
class AppendSuite extends AbstractDFDSuite {

  import DFDComponent._

  it("can automatically generate names") {

    val flow = (
      'input
        :>> new Tokenizer()
        :=>> new Tokenizer()
        :-> new Tokenizer()
        :>> new Tokenizer()
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   +- > ForwardNode  [input$Tokenizer] > Tokenizer > [input$Tokenizer$Tokenizer]
        |      +- > ForwardNode  [input$Tokenizer$Tokenizer] > Tokenizer > [input$Tokenizer$Tokenizer$Tokenizer]
        |         +- > ForwardNode (HEAD)(<TAIL) [input$Tokenizer$Tokenizer$Tokenizer] > Tokenizer > [input$Tokenizer$Tokenizer$Tokenizer$Tokenizer]
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [input$Tokenizer$Tokenizer$Tokenizer] > Tokenizer > [input$Tokenizer$Tokenizer$Tokenizer$Tokenizer]
      """.stripMargin
      )
  }

  it("pincer topology can be defined by A :-> B <-: A") {
    val input: DFDComponent = 'input
    val flow = input :-> new VectorAssembler() <-: input

    flow
      .show(showID = false, forward = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |< BackwardNode (HEAD) [input,input] > VectorAssembler > [input$VectorAssembler]
        |:- < BackwardNode (TAIL) [input]
        |+- < BackwardNode (TAIL) [input]
      """.stripMargin
      )
  }

  it("A :-> B :-> Source is associative") {
    val flow1 = 'input :-> new Tokenizer() :-> 'dummy // resolve to rebase then union
    val flow2 = 'input :-> (new Tokenizer() :-> 'dummy) // resolve to union then rebase
    flow1
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(flow2.show(showID = false, compactionOpt = compactionOpt))
  }

  it("A <-: B <-: Source is associative") {
    val flow1 = 'dummy <-: new Tokenizer() <-: 'input
    val flow2 = 'dummy <-: (new Tokenizer() <-: 'input)
    flow1
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(flow2.show(showID = false, compactionOpt = compactionOpt))
  }

  it("A :-> B :-> detached Stage is associative") {
    val flow1 = 'input :-> new Tokenizer() :-> new NGram() // resolve to rebase then union
    val flow2 = 'input :-> (new Tokenizer() :-> new NGram()) // resolve to union then rebase
    flow1
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(flow2.show(showID = false, compactionOpt = compactionOpt))
  }

  it("A <-: B <-: detached Stage is associative") {
    val flow1 = new NGram() <-: new Tokenizer() <-: 'input
    val flow2 = new NGram() <-: (new Tokenizer() <-: 'input)
    flow1
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(flow2.show(showID = false, compactionOpt = compactionOpt))
  }

  it(":-> Stage is cast to rebase") {

    val flow = (
      (
        'input
          :-> new Tokenizer()
          :-> new StopWordsRemover()
      ).from("Tokenizer")
        .and("StopWordsRemover")
        :-> new NGram()
        :-> new NGram()
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   :- > ForwardNode (HEAD) [input$Tokenizer] > NGram > [input$Tokenizer$NGram]
        |   :  +- > ForwardNode  [] > SQLTransformer > []
        |   +- > ForwardNode  [input$Tokenizer] > StopWordsRemover > [input$Tokenizer$StopWordsRemover]
        |      +- > ForwardNode (HEAD)(<TAIL) [input$Tokenizer$StopWordsRemover] > NGram > [input$Tokenizer$StopWordsRemover$NGram]
        |         +- > ForwardNode  [] > SQLTransformer > []
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [input$Tokenizer$StopWordsRemover] > NGram > [input$Tokenizer$StopWordsRemover$NGram]
        |+- > ForwardNode  [] > SQLTransformer > []
      """.stripMargin
      )
  }

  it("<-: Stage is cast to rebase") {

    val flow = (
      new NGram()
        <-: new NGram()
        <-: (
        new StopWordsRemover() <-: new Tokenizer() <-: 'input
      ).from("Tokenizer")
        .and("StopWordsRemover")
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (HEAD)(TAIL>) [input$Tokenizer$StopWordsRemover] > NGram > [input$Tokenizer$StopWordsRemover$NGram]
        |+- > ForwardNode  [] > SQLTransformer > []
        |/ right <
        |> ForwardNode (<TAIL) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   :- > ForwardNode (HEAD) [input$Tokenizer] > NGram > [input$Tokenizer$NGram]
        |   :  +- > ForwardNode  [] > SQLTransformer > []
        |   +- > ForwardNode  [input$Tokenizer] > StopWordsRemover > [input$Tokenizer$StopWordsRemover]
        |      +- > ForwardNode (HEAD)(TAIL>) [input$Tokenizer$StopWordsRemover] > NGram > [input$Tokenizer$StopWordsRemover$NGram]
        |         +- > ForwardNode  [] > SQLTransformer > []
      """.stripMargin
      )
  }

  it(":-> Source is cast to union") {
    val flow = 'input :-> new Tokenizer() :-> 'dummy
    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input]
        |+- > ForwardNode (HEAD)(<TAIL) [input] > Tokenizer > [input$Tokenizer]
        |> ForwardNode (HEAD)(TAIL) [dummy]
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [input] > Tokenizer > [input$Tokenizer]
        |> ForwardNode (HEAD)(TAIL) [dummy]
      """.stripMargin
      )
  }

  it("<-: Source is cast to union") {
    val flow = 'dummy <-: new Tokenizer() <-: 'input
    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (HEAD)(TAIL>) [input] > Tokenizer > [input$Tokenizer]
        |> ForwardNode (HEAD)(TAIL) [dummy]
        |/ right <
        |> ForwardNode (<TAIL) [input]
        |+- > ForwardNode (HEAD)(TAIL>) [input] > Tokenizer > [input$Tokenizer]
        |> ForwardNode (HEAD)(TAIL) [dummy]
      """.stripMargin
      )
  }
}

class AppendSuite_PruneDownPath extends AppendSuite with UsePruneDownPath

class AppendSuite_PruneDownPathKeepRoot extends AppendSuite with UsePruneDownPathKeepRoot
