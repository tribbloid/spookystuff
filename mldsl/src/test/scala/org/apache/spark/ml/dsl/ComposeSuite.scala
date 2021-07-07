package org.apache.spark.ml.dsl

import org.apache.spark.ml.feature._

class ComposeSuite extends AbstractDFDSuite {

  import DFDComponent._

  it("compose_> Source doesn't work") {
    intercept[IllegalArgumentException] {
      'input :>> new Tokenizer() :>> 'dummy
    }
  }

  it("compose_< Source doesn't work") {
    intercept[IllegalArgumentException] {
      'input :>> new Tokenizer() :>> 'dummy
    }
  }

  it("compose_> PASSTHROUGH doesn't change the flow") {
    val flow = 'input :>> new Tokenizer() :>> PASSTHROUGH
    val flow2 = 'input :>> new Tokenizer()
    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        flow2.show(showID = false, compactionOpt = compactionOpt)
      )
  }

  it("PASSTHROUGH compose_> Stage doesn't change the flow") {
    val flow1 = 'input :>> (PASSTHROUGH :>> new Tokenizer())
    val flow2 = 'input :>> new Tokenizer()
    flow1
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        flow2.show(showID = false, compactionOpt = compactionOpt)
      )
  }

  it("compose_> (PASSTHROUGH || Stage) generates 2 heads") {
    val flow = (
      'input
        :-> new Tokenizer()
        :>> (
          PASSTHROUGH U
            new StopWordsRemover()
        )
        :=>> new HashingTF()
    )
    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   :- > ForwardNode (HEAD) [input$Tokenizer] > HashingTF > [input$Tokenizer$HashingTF]
        |   +- > ForwardNode  [input$Tokenizer] > StopWordsRemover > [input$Tokenizer$StopWordsRemover]
        |      +- > ForwardNode (HEAD)(<TAIL) [input$Tokenizer$StopWordsRemover] > HashingTF > [input$Tokenizer$StopWordsRemover$HashingTF]
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [input$Tokenizer$StopWordsRemover] > HashingTF > [input$Tokenizer$StopWordsRemover$HashingTF]
      """.stripMargin
      )
  }

  it("declare API is equally effective") {
    val flow1 = (
      new VectorAssembler()
        <<: (new HashingTF()
        <<=: (
        PASSTHROUGH
          U new StopWordsRemover()
      )
        <<: new Tokenizer()
        <<: 'input)
    )
    val part1 = declare(
      new Tokenizer() <<: 'input
    )
    val part2: DFDComponent = new HashingTF()
    val part3: DFDComponent = new VectorAssembler()

    val flow2 = declare(
      part3 <<: part2 <<: part1,
      part3 <<: part2.replicate() <<: new StopWordsRemover() <<: part1
    )

    //    val flow3 = declare(
    //      part3 < part2 < part1,
    //      part3 < part2.replicate("_2") < new StopWordsRemover() < part1
    //    )

    flow1
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        flow2.show(showID = false, compactionOpt = compactionOpt)
      )
  }

  it("result of compose_> can be the first operand of compose_<") {
    val flow = new VectorAssembler() <<: (
      'input :>> new Tokenizer() :>> new HashingTF()
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (HEAD)(TAIL>) [input$Tokenizer$HashingTF] > VectorAssembler > [input$Tokenizer$HashingTF$VectorAssembler]
        |> ForwardNode (TAIL>) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   +- > ForwardNode (<TAIL) [input$Tokenizer] > HashingTF > [input$Tokenizer$HashingTF]
        |      +- > ForwardNode (HEAD)(TAIL>) [input$Tokenizer$HashingTF] > VectorAssembler > [input$Tokenizer$HashingTF$VectorAssembler]
        |/ right <
        |> ForwardNode (<TAIL) [input$Tokenizer] > HashingTF > [input$Tokenizer$HashingTF]
        |+- > ForwardNode (HEAD)(TAIL>) [input$Tokenizer$HashingTF] > VectorAssembler > [input$Tokenizer$HashingTF$VectorAssembler]
      """.stripMargin)
    flow.show(showID = false, compactionOpt = compactionOpt, asciiArt = true).treeNodeShouldBe()
  }

  it("result of compose_< can be the first operand of compose_>") {
    val flow = (
      new HashingTF() <<: new Tokenizer() <<: 'input
        :>> new VectorAssembler()
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input$Tokenizer] > HashingTF > [input$Tokenizer$HashingTF]
        |+- > ForwardNode (HEAD)(<TAIL) [input$Tokenizer$HashingTF] > VectorAssembler > [input$Tokenizer$HashingTF$VectorAssembler]
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [input$Tokenizer$HashingTF] > VectorAssembler > [input$Tokenizer$HashingTF$VectorAssembler]
        |> ForwardNode (<TAIL) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   +- > ForwardNode (TAIL>) [input$Tokenizer] > HashingTF > [input$Tokenizer$HashingTF]
        |      +- > ForwardNode (HEAD)(<TAIL) [input$Tokenizer$HashingTF] > VectorAssembler > [input$Tokenizer$HashingTF$VectorAssembler]
      """.stripMargin
      )
    flow.show(showID = false, compactionOpt = compactionOpt, asciiArt = true).treeNodeShouldBe()
  }

  it("A compose_> (PASSTHROUGH || Stage) rebase_> B is associative") {
    val flow1 = (
      new Tokenizer()
        :>> (
          PASSTHROUGH U
            new StopWordsRemover()
        )
        :=>> new HashingTF()
    )
    val flow2 = (
      new Tokenizer()
        :>> ((
          PASSTHROUGH U
            new StopWordsRemover()
        )
          :=>> new HashingTF())
    )

    flow1
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        flow2.show(showID = false, compactionOpt = compactionOpt)
      )
  }

  it("compose_> can append a stage to 2 heads") {
    val flow = (
      ('input1 U 'input2)
        :>> new VectorAssembler()
    )

    flow
      .show(showID = false)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input1]
        |+- > ForwardNode (HEAD)(<TAIL) [input1,input2] > VectorAssembler > [VectorAssembler]
        |> ForwardNode (TAIL>) [input2]
        |+- > ForwardNode (HEAD)(<TAIL) [input1,input2] > VectorAssembler > [VectorAssembler]
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [input1,input2] > VectorAssembler > [VectorAssembler]
      """.stripMargin
      )
  }

  it("compose_< can append a stage to 2 heads") {

    val flow = (
      new VectorAssembler()
        <<: ('input1 U 'input2)
    )

    flow
      .show(showID = false)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (HEAD)(TAIL>) [input1,input2] > VectorAssembler > [VectorAssembler]
        |/ right <
        |> ForwardNode (<TAIL) [input1]
        |+- > ForwardNode (HEAD)(TAIL>) [input1,input2] > VectorAssembler > [VectorAssembler]
        |> ForwardNode (<TAIL) [input2]
        |+- > ForwardNode (HEAD)(TAIL>) [input1,input2] > VectorAssembler > [VectorAssembler]
      """.stripMargin
      )
  }

  it("compose_> can append a stage to 2 heads from 1 tail") {

    val flow = (
      'input
        :-> new Tokenizer()
        :=>> (
          PASSTHROUGH
            U new StopWordsRemover()
        )
        :=>> new HashingTF()
        :>> new VectorAssembler()
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   :- > ForwardNode  [input$Tokenizer] > HashingTF > [input$Tokenizer$HashingTF]
        |   :  +- > ForwardNode (HEAD)(<TAIL) [input$Tokenizer$HashingTF,input$Tokenizer$StopWordsRemover$HashingTF] > VectorAssembler > [input$Tokenizer$HashingTF$VectorAssembler]
        |   +- > ForwardNode  [input$Tokenizer] > StopWordsRemover > [input$Tokenizer$StopWordsRemover]
        |      +- > ForwardNode  [input$Tokenizer$StopWordsRemover] > HashingTF > [input$Tokenizer$StopWordsRemover$HashingTF]
        |         +- > ForwardNode (HEAD)(<TAIL) [input$Tokenizer$HashingTF,input$Tokenizer$StopWordsRemover$HashingTF] > VectorAssembler > [input$Tokenizer$HashingTF$VectorAssembler]
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [input$Tokenizer$HashingTF,input$Tokenizer$StopWordsRemover$HashingTF] > VectorAssembler > [input$Tokenizer$HashingTF$VectorAssembler]
      """.stripMargin
      )
  }

  it("compose_< can append a stage to 2 heads from 1 tail") {

    val flow = (
      new VectorAssembler()
        <<: new HashingTF()
        <<=: (
        PASSTHROUGH
          U new StopWordsRemover()
      )
        <<=: new Tokenizer()
        <-: 'input
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (HEAD)(TAIL>) [input$Tokenizer$StopWordsRemover$HashingTF] > VectorAssembler > [input$Tokenizer$StopWordsRemover$HashingTF$VectorAssembler]
        |/ right <
        |> ForwardNode (<TAIL) [input]
        |+- > ForwardNode  [input] > Tokenizer > [input$Tokenizer]
        |   :- > ForwardNode  [input$Tokenizer] > HashingTF > [input$Tokenizer$HashingTF]
        |   :  +- > ForwardNode (HEAD) [input$Tokenizer$HashingTF] > VectorAssembler > [input$Tokenizer$HashingTF$VectorAssembler]
        |   +- > ForwardNode  [input$Tokenizer] > StopWordsRemover > [input$Tokenizer$StopWordsRemover]
        |      +- > ForwardNode  [input$Tokenizer$StopWordsRemover] > HashingTF > [input$Tokenizer$StopWordsRemover$HashingTF]
        |         +- > ForwardNode (HEAD)(TAIL>) [input$Tokenizer$StopWordsRemover$HashingTF] > VectorAssembler > [input$Tokenizer$StopWordsRemover$HashingTF$VectorAssembler]
      """.stripMargin
      )
  }

  it("compose_> can append a stage to merged heads") {
    val flow = (
      ('input1 U 'input2)
        :>> new VectorAssembler()
        :>> new IndexToString()
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input1]
        |+- > ForwardNode  [input1,input2] > VectorAssembler > [VectorAssembler]
        |   +- > ForwardNode (HEAD)(<TAIL) [VectorAssembler] > IndexToString > [VectorAssembler$IndexToString]
        |> ForwardNode (TAIL>) [input2]
        |+- > ForwardNode  [input1,input2] > VectorAssembler > [VectorAssembler]
        |   +- > ForwardNode (HEAD)(<TAIL) [VectorAssembler] > IndexToString > [VectorAssembler$IndexToString]
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [VectorAssembler] > IndexToString > [VectorAssembler$IndexToString]
      """.stripMargin
      )
  }

  it("compose_< can append a stage to merged heads") {

    val flow = (
      new IndexToString()
        <<: new VectorAssembler()
        <<: ('input1 U 'input2)
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (HEAD)(TAIL>) [VectorAssembler] > IndexToString > [VectorAssembler$IndexToString]
        |/ right <
        |> ForwardNode (<TAIL) [input1]
        |+- > ForwardNode  [input1,input2] > VectorAssembler > [VectorAssembler]
        |   +- > ForwardNode (HEAD)(TAIL>) [VectorAssembler] > IndexToString > [VectorAssembler$IndexToString]
        |> ForwardNode (<TAIL) [input2]
        |+- > ForwardNode  [input1,input2] > VectorAssembler > [VectorAssembler]
        |   +- > ForwardNode (HEAD)(TAIL>) [VectorAssembler] > IndexToString > [VectorAssembler$IndexToString]
      """.stripMargin
      )
  }

  it("compose_> can bypass Source of downstream") {
    val flow = (
      'input
        :>> (
          'dummy :>>
            new Tokenizer()
        )
    )

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (TAIL>) [input]
        |+- > ForwardNode (HEAD)(<TAIL) [input] > Tokenizer > [input$Tokenizer]
        |/ right <
        |> ForwardNode (HEAD)(<TAIL) [input] > Tokenizer > [input$Tokenizer]
      """.stripMargin
      )
  }

  it("compose_< can bypass Source of downstream") {
    val flow = (
      new Tokenizer() <<:
        'dummy
    ) <<:
      'input

    flow
      .show(showID = false, compactionOpt = compactionOpt)
      .treeNodeShouldBe(
        """
        |\ left >
        |> ForwardNode (HEAD)(TAIL>) [input] > Tokenizer > [input$Tokenizer]
        |/ right <
        |> ForwardNode (<TAIL) [input]
        |+- > ForwardNode (HEAD)(TAIL>) [input] > Tokenizer > [input$Tokenizer]
      """.stripMargin
      )
  }

  //  test("from can select by name") {
  //
  //    "input")
  //  }
  //
  //  test("from can select by qualified name") {
  //
  //  }
  //
  //  test("from can select by * wildcard qualified name") {
  //
  //  }
  //
  //  test("from can select by ** wildcard qualified name") {
  //
  //  }

  it("Merge works when operand2 is type consistent") {

    val flow = (
      'input.string
        :>> new Tokenizer()
        :>> new StopWordsRemover()
    )

    flow.show(showID = false, compactionOpt = compactionOpt).treeNodeShouldBe()
  }

  it("Merge throws an exception when operand2 is type inconsistent with output of operand1 as a Source") {

    intercept[IllegalArgumentException] {
      (
        'input.string
          :>> new VectorAssembler()
      )
    }
  }

  it("Merge throws an exception when operand2 is type inconsistent with output of operand1 as a Flow") {

    intercept[IllegalArgumentException] {
      (
        'input.string
          :>> new Tokenizer()
          :>> new VectorAssembler()
      )
    }
  }

  it("Union throws an exception when a stage in result is type inconsistent") {

    val part1 = declare(
      new Tokenizer() <<: 'input.string
    )
    val part2: DFDComponent = new HashingTF()
    val part3: DFDComponent = new StopWordsRemover()

    intercept[IllegalArgumentException] {
      (part2 <<: part1) U
        (part3 <<: part2)
    }
  }

  it("Union throws an exception when a stage in result has incompatible number of inputCols") {

    val part1 = declare(
      new Tokenizer() <<: 'input.string
    )
    val part2: DFDComponent = new HashingTF()
    val part3: DFDComponent = new IDF()

    intercept[IllegalArgumentException] {
      (part3 <<: part2 <<: part1) U
        (part3 <<: part2.replicate() <<: new StopWordsRemover() <<: part1)
    }
  }
}

class ComposeSuite_PruneDownPath extends ComposeSuite with UsePruneDownPath

class ComposeSuite_PruneDownPathKeepRoot extends ComposeSuite with UsePruneDownPathKeepRoot
