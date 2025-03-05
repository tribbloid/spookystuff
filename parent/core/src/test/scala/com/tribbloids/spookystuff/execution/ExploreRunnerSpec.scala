package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.{NoOp, Wget}
import com.tribbloids.spookystuff.row.Data
import com.tribbloids.spookystuff.testutils.{SpookyBaseSpec, UnpackResources}
import com.tribbloids.spookystuff.rdd.DataView

class ExploreRunnerSpec extends SpookyBaseSpec {

  import com.tribbloids.spookystuff.testutils.FileDocsFixture.*

  describe("on local dir") {

    val entry: DataView[Unit] = spooky
      .fetch { _ =>
        Wget(DEEP_DIR_URL)
      }

    it("explore then select") {

      val ds = entry
        .recursively()
        .explore { row =>
          val docs = row.docs
          val dirs = docs.\("root directory")
          val path = dirs.\("path").text

          val action = path match {
            case Some(p) => Wget(p)
            case None    => NoOp
          }
          //        print_@(s"reading ${action}")

          action
        }
        .select { row =>
          val docs = row.docs
          val dirs = docs.\("root directory")
          dirs.\("path").text
        }

      val plan = Option(ds.plan).collect {
        case v: ExplorePlan[Data.Exploring[Unit], Option[String]] => v
      }.get

      val runner = {

        val state0 = plan.state0RDD.collect()
        val runner = ExploreRunner(state0.iterator, plan.pathPlanningImpl, plan.sameBy)
        runner
      }

      val executed =
        runner.Run(plan.fn).recursively(50).toSeq

      val content = executed
        .flatMap {
          case (group, state) =>
            val data = state.visited.toSeq.flatten

            data.map { v =>
              (v.depth, group.trace, v.raw)
            }
        }
        .sortBy(_._1)

      val relativePaths = content.map {
        case (i, from, to) =>
          (
            i,
            from.toString.replace(UnpackResources.ROOT_DIR, ""),
            to.getOrElse("").replace(UnpackResources.ROOT_DIR, "")
          )
      }

      relativePaths
        .mkString("\n")
        .shouldBe(
          """
            |(0,{ Wget(/testutils/dir,MustHaveTitle) },file:/testutils/dir/dir)
            |(1,{ Wget(file:/testutils/dir/dir,MustHaveTitle) },file:/testutils/dir/dir/dir)
            |(2,{ Wget(file:/testutils/dir/dir/dir,MustHaveTitle) },file:/testutils/dir/dir/dir/dir)
            |(3,{ Wget(file:/testutils/dir/dir/dir/dir,MustHaveTitle) },)
            |""".stripMargin
        )
    }

    it("select then explore") {

      val ds = entry
        .recursively()
        .select { row =>
          val docs = row.docs
          val dirs = docs.\("root directory")
          dirs.\("path").text
        }
        .explore { row =>
          val action = row.data.raw match {
            case Some(p) => Wget(p)
            case None    => NoOp
          }

          action
        }

      val plan = Option(ds.plan).collect {
        case v: ExplorePlan[Data.Exploring[Unit], Option[String]] => v
      }.get

      val runner = {

        val state0 = plan.state0RDD.collect()
        val runner = ExploreRunner(state0.iterator, plan.pathPlanningImpl, plan.sameBy)
        runner
      }

      val executed =
        runner.Run(plan.fn).recursively(50).toSeq

      val content = executed
        .flatMap {
          case (group, state) =>
            val data = state.visited.toSeq.flatten

            data.map { v =>
              (v.depth, group.trace, v.raw)
            }
        }
        .sortBy(_._1)

      val relativePaths = content.map {
        case (i, from, to) =>
          (
            i,
            from.toString.replace(UnpackResources.ROOT_DIR, ""),
            to.getOrElse("").replace(UnpackResources.ROOT_DIR, "")
          )
      }

      relativePaths
        .mkString("\n")
        .shouldBe(
          """
            |(0,{ Wget(/testutils/dir,MustHaveTitle) },file:/testutils/dir/dir)
            |(1,{ Wget(file:/testutils/dir/dir,MustHaveTitle) },file:/testutils/dir/dir/dir)
            |(2,{ Wget(file:/testutils/dir/dir/dir,MustHaveTitle) },file:/testutils/dir/dir/dir/dir)
            |(3,{ Wget(file:/testutils/dir/dir/dir/dir,MustHaveTitle) },)
            |""".stripMargin
        )
    }
  }
}
