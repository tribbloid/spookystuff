package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.uav.UAVTestUtils
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.sim.{APMSITLFixture, QuadSimFactory}
import com.tribbloids.spookystuff.uav.spatial.NED

import scala.util.Random

/**
  * Created by peng on 28/03/17.
  */
class GASolveIT extends APMSITLFixture {

  override lazy val simFactory = QuadSimFactory(0)

  val wps: Seq[Waypoint] = UAVTestUtils.LawnMowerPattern(
    parallelism * 3,
    NED(10, 10, -10),
    NED(100, 0, 0),
    NED(0, 20, -2)
  )
    .wpActions

  val solver = GASolver(
    wps.map{ a => List(a)}.toList,
    this.spooky
  )

  def seeds = solver.generateSeedPairs(200, 200)

  test("seed population should include all UAVs in the pool") {

    val uavs = seeds.map {
      tuple =>
        tuple._1.uav
    }
      .collect()
      .distinct
    uavs
      .map(_.uris.head)
      .mkString("\n")
      .shouldBe(
        simURIs.mkString("\n"),
        sort = true
      )
  }

  test("each hypothesis in the seed population should cover all indices") {

    val iss = seeds.map(_._2)
      .collect()
    (0 until 200).foreach {
      i =>
        val is = iss.flatMap(_.apply(i)).toSeq.sorted
        assert(is == (0 until 200))
    }
  }

  lazy val hypotheses = solver.generateSeeds(2)

  def tt(name: String)(
    fn: solver.Hypothesis => solver.Hypothesis
  ): Unit = {

    test(s"hypothesis after N $name(s) still covers all indices") {
      val first = hypotheses.head

      assertCoverAllIndices(first)

      var transformed = first
      for (i <- 0 to (5 + Random.nextInt(5))) {
        println(s"$name $i time(s)")
        transformed = fn(transformed)
      }

      assertCoverAllIndices(transformed)
    }
  }

  private def assertCoverAllIndices(hypothesis: solver.Hypothesis) = {
    val is = hypothesis.rdd.flatMap(_.is).collect().toSeq.sorted
    assert(is == wps.indices)
  }

  tt("swap") {
    v => v.copy(rdd = solver.swap(v.rdd))
  }

  tt("insert") {
    v => v.copy(rdd = solver.insert(v.rdd))
  }

  tt("mutation") {
    v => solver.Mutation.mutate(v).asInstanceOf[solver.Hypothesis]
  }

  tt("crossover") {
    v =>
      var second = hypotheses.last
      val neo = solver.Crossover.crossover(v, second)
      second = neo.getSecond.asInstanceOf[solver.Hypothesis]
      assertCoverAllIndices(second)

      neo.getFirst.asInstanceOf[solver.Hypothesis]
  }

  //  tt("crossover") {
  //    v => solver.Cro.mutate(v).asInstanceOf[solver.Hypothesis]
  //  }

  //  test("showdown") {
  //
  //    import scala.collection.JavaConverters._
  //
  //    val seeds: util.List[Chromosome] = solver.generateSeeds(200).map(v => v: Chromosome).asJava
  //
  //    // initialize a new genetic algorithm
  //    val ga = new GeneticAlgorithm(
  //      new OnePointCrossover<Integer>(),
  //      1,
  //      new RandomKeyMutation(),
  //      0.10,
  //      new TournamentSelection(TOURNAMENT_ARITY)
  //    )
  //
  //    val initial: ElitisticListPopulation = new ElitisticListPopulation(seeds, 200, 0.05)
  //
  //    // stopping condition
  //    val stopCond = new FixedGenerationCount(200)
  //
  //    // run the algorithm
  //    val finalPopulation = ga.evolve(initial, stopCond)
  //
  //    // best chromosome from the final population
  //    val bestFinal = finalPopulation.getFittestChromosome
  //  }
}
