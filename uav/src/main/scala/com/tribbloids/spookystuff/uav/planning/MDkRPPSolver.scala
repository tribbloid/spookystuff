package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.uav.actions.UAVPositioning
import com.tribbloids.spookystuff.uav.spatial.LocationGlobal
import org.apache.commons.math3.genetics._

import scala.collection.immutable.Seq

case class Path(
                 start: LocationGlobal,
                 cost: Double,
                 end: LocationGlobal
               )

case class Trace2Paths(
                        trace: Trace
                      ) {

  def paths(costFn: Seq[UAVPositioning] => Double): Seq[Path] = {
    ???
  }
}

/**
  * Created by peng on 07/02/17.
  */
// Multi-Depot k-Rural Postman Problem
// genetic algorithm really come in handy.
object MDkRPPSolver {


  //all solutions refers to the same matrix
  case class Solution(
                       paths: Seq[Seq[UAVPositioning]]
                       //travelling path of each drone.
                       //outer index indicates group, inner index indicates index of the edges being traversed.
                       //include start and end index, start index is hardcoded and cannot be changed.
                       //the last 3 parameters must have identical cadinality
                       //total distance can be easily calculated
                     ) extends Chromosome {

    override def fitness(): Double = {
      paths.map {
        path =>
          path.map {
            i =>
              ???
          }
      }
      ???
    }
  }

  case class Selection() extends SelectionPolicy {

    override def select(population: Population): ChromosomePair = ???
  }
  case class Mutation() extends MutationPolicy {

    override def mutate(original: Chromosome): Chromosome = ???
  }
  case class Crossover() extends CrossoverPolicy {
    override def crossover(first: Chromosome, second: Chromosome): ChromosomePair = ???
  }
}
